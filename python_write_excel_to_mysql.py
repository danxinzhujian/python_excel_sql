# -*- coding: utf-8 -*-
import xlrd
import pymysql
import threading
import datetime
import os
import re


# 生成用于生成表格的sql
def generate_create_table_sql(column_name_list):
    table_sql = 'create table table_all ('
    if len(column_name_list) > 0:
        table_sql = table_sql + column_name_list[0] + ' varchar(255)'
    if len(column_name_list) > 1:
        for column_name in column_name_list[1:]:
            table_sql = table_sql + ', ' + column_name + ' varchar(255)'
        table_sql = table_sql + ');'
    return table_sql


# 生成用于将数据插入表格的sql
def generate_insert_table_sql(column_name_list):
    insert_table_sql_part1 = 'insert into table_all ('
    insert_table_sql_part2 = ' values('
    if len(column_name_list) > 0:
        insert_table_sql_part1 += column_name_list[0]
        insert_table_sql_part2 += '%s'
    if len(column_name_list) > 1:
        for column_name in column_name_list[1:]:
            insert_table_sql_part1 = insert_table_sql_part1 + ', ' + column_name
            insert_table_sql_part2 = insert_table_sql_part2 + ', ' + '%s'
        insert_table_sql_part1 += ')'
        insert_table_sql_part2 += ');'
    return insert_table_sql_part1 + insert_table_sql_part2


# 获取列名，并返回所有列名的列表
def read_column_names(excel_file_name):
    column_name_list = []
    # 打开excel
    data = xlrd.open_workbook(excel_file_name)
    sheet = data.sheet_by_index(0)
    for col_id in range(sheet.ncols):
        column_name_list.append(sheet.row(0)[col_id].value)
    return column_name_list


# 读取excel里的记录，并存入到mysql
def read_excel_write_mysql(excel_file_name, _host, _port, _user, _password, _database, thread_id):
    column_name_list = read_column_names(excel_file_name)
    # create_table_sql = generate_create_table_sql(column_name_list)
    insert_table_sql = generate_insert_table_sql(column_name_list)

    # 打开mysql数据库
    conn = pymysql.connect(host=_host, port=_port, user=_user, password=_password, database=_database, charset='utf8')
    cursor = conn.cursor()
    # cursor.execute(create_table_sql)

    # 打开excel
    data = xlrd.open_workbook(excel_file_name)

    # sheets = data.sheet_names()
    for sheet_id in range(data.nsheets):
        sheet = data.sheet_by_index(sheet_id)
        row_number = sheet.nrows
        col_number = sheet.ncols
        print('列数: ' + str(col_number))

        # count 用于统计行数，更重要的是用于批量提交：count为1000的倍数则执行数据库提交
        count = 0
        # for循环内逐条插入记录
        for row_id in range(1, row_number):
            # parameter_list 存储一列数据，并作为数据库插入语句的参数
            parameter_list = []
            for col_id in range(col_number):
                parameter_list.append(str(sheet.row(row_id)[col_id].value))

            cursor.execute(insert_table_sql, tuple(parameter_list))
            count += 1
            if (count % 1000) == 0:
                conn.commit()
                print("线程" + str(thread_id) + "插入第" + str(count) + "条记录！")


    cursor.close()
    conn.close()


# 获取当前目录下所有的excel文件
# 判断标准：excel文件名包含xls或者包含XLS
# 如果excel文件识别不对，请修改判断标准
def get_current_directory_excel_files():
    excel_file_list = []
    path = os.getcwd()
    file_or_dir_list = os.listdir(path)
    for file in file_or_dir_list:
        if file.find("xls") >= 0 or file.find("XLS") >= 0:
            excel_file_list.append(file)
    return excel_file_list


if __name__ == '__main__':
    ####################################################################################################################
    # 待修改：在自己电脑上运行一般只需要修改_user、_password、_database
    ####################################################################################################################
    _host = "localhost"
    _port = 3306
    _user = "root"
    _password = "123456"
    _database = "database_name"
    ####################################################################################################################

    start_time = datetime.datetime.now()

    excel_file_name_list = get_current_directory_excel_files()
    column_name_list = read_column_names(excel_file_name_list[0])
    create_table_sql = generate_create_table_sql(column_name_list)
    # 打开mysql数据库
    conn = pymysql.connect(host=_host, port=_port, user=_user, password=_password, database=_database, charset='utf8',
                           autocommit=True)
    cursor = conn.cursor()

    # # 查询数据库中是否存在数据库表table_all
    # sql = "show tables;"
    # cursor.execute(sql)
    # tables = cursor.fetchall()
    # tables_list = re.findall('(\'.*?\')', str(tables))
    # tables_list = [re.sub("'", '', each) for each in tables_list]
    # if 'table_all' in tables_list:
    #     # 数据库中已存在数据库表table_all，先删除
    #     cursor.execute('DROP TABLE table_all;')
    #     print("已删除旧的数据库表table_all!")

    # 数据库中已存在数据库表table_all，先删除
    cursor.execute('DROP TABLE IF EXISTS table_all;')

    # 新建数据库表，表名为table_all
    cursor.execute(create_table_sql)
    print("新建数据库表成功！")
    cursor.close()
    conn.close()

    threads = []
    for excel_file_name in excel_file_name_list:
        # read_excel_write_mysql(excel_file_name, _host, _port, _user, _password, _database)
        # 创建多个线程
        threads.append(threading.Thread(target=read_excel_write_mysql, args=(excel_file_name, _host, _port, _user, _password, _database, len(threads))))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print("运行时间：" + str(run_time.total_seconds()) + "秒！")
    print("合并excel文件包括：")
    for file in excel_file_name_list:
        print("  " + file)
    print("合并excel成功！MySQL数据库表名为table_all！")
