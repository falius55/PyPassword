#! /usr/bin/python3.4
# -*- coding: utf-8 -*-
import MySQLdb.cursors
from configparser import ConfigParser

"""
データベースに問い合わせるためのクラス

使い方はファイル下部にあるif __name__ == '__main__'句を参照してください
"""

INIT_FILE_PATH = './config.ini'


class MySql(object):

    # enterよりinitの方が先に呼ばれる
    def __init__(self, **args):
        """
        引数にはデータベースの設定情報を渡しますが、必ずしもすべてを指定する必要はありません
        設定ファイル(config.ini)のセクション名を渡すことで補うことができます
        逆に、セクション名を渡しても個別に情報を渡すことを妨げません
        個別に渡された情報が最優先され、渡されていない種類の情報を設定ファイルで補います
        設定ファイル内におけるセクション名はconnect()で指定することもできますが、デフォルトでは設定ファイルの[default]が利用されます
        なお、セクション名を指定した場合は個別に渡された引数および指定されたセクションに情報がない項目があってもdefaultセクションが補うことはありません

        key user データベースのユーザー名
        key host データベースのホスト名
        key dbname データベース名
        key passwd パスワード名
        key init_section 上記パラメータを定義した設定ファイルのセクション名
        """

        if 'user' in args:
            self.user = args.get('user')
        if 'host' in args:
            self.host = args.get('host')
        if 'dbname' in args:
            self.dbname = args.get('dbname')
        if 'passwd' in args:
            self.passwd = args.get('passwd')
        if 'init_section' in args:
            self.init_section = args.get('init_section')
        if 'init_file' in args:
            self.init_file = args.get('init_file')

    def __enter__(self):
        self.connect('default' if not hasattr(self, 'init_section') else self.init_section)
        return self

    def __exit__(self, excType, excValue, traceback):
        self.close()
        return False

    def __iter__(self):
        """
        最新結果の各行の列名から値へのディクショナリをイテレートするイテレータを返します
        現在のカーソル位置への影響はありません
        """
        return self.resultTuple.__iter__()

    def close(self):
        """
        終了処理を行います
        with文を使わずに利用している場合には明確に呼び出す必要があります
        """
        self.commit()
        self.cursor.close()
        self.connector.close()

    def connect(self, section=None):
        """
        データベースに接続します
        __init__で指定されなかった属性は設定ファイルの値を使用します
        @param 設定ファイルの使用するセクション名
        """
        if section is None:
            section = 'default' if not hasattr(self, 'init_section') else self.init_section

        inifile = ConfigParser()
        inifile.read(INIT_FILE_PATH if not hasattr(self, 'init_file') else self.init_file)
        if not hasattr(self, 'user'):
            self.user = inifile[section]['user']
        if not hasattr(self, 'host'):
            self.host = inifile[section]['host']
        if not hasattr(self, 'dbname'):
            self.dbname = inifile[section]['dbname']
        if not hasattr(self, 'passwd'):
            self.passwd = inifile[section]['passwd']
        self.connector = MySQLdb.connect(host=self.host, db=self.dbname, user=self.user, passwd=self.passwd, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
        self.cursor = self.connector.cursor()
        return self

    def update(self, sql, holder=None):
        """
        結果を伴わない更新処理全般を行います(insert, update etc)
        """
        if holder is None:
            holder = ()
        print('execute update:', sql, ',', holder)
        self.cursor.execute(sql, holder)
        return self

    def updateSet(self, tableName, columns, values, where):
        """
        条件に合致するレコードの値を更新します
        条件式の値が文字列である場合にはダブルクオーテーションで囲むのを忘れないでください
        @param tableName 更新するテーブル名
        @param columns 更新する列名のタプル
        @param values columnsに対応する順序で、更新データ
        @param where 更新するレコードを指定する条件式
        """
        sets = []
        for column, value in zip(columns, values):
            sets.append(column + '=' + self._format(value))
        sql = 'update '
        sql += tableName
        sql += ' set '
        sql += ','.join(sets)
        sql += ' where ' + where
        self.update(sql)

    def delete(self, tableName, where):
        """
        条件に合致するレコードを削除します
        条件式の値が文字列である場合にはダブルクオーテーションで囲むのを忘れないでください
        @param tableName 削除レコードのあるテーブル名
        @param where 条件式　ex.'name="sample_name"'
        """
        sql = 'delete from ' + tableName
        sql += ' where ' + where
        self.update(sql)

    def insert(self, tableName, columns, values):
        """
        指定されたテーブルに新しいレコードを追加します
        @param tableName 追加先のテーブル名
        @param columns 追加する列名のタプル
        @param values columnsに対応する形で、追加するデータのタプル
        """
        strColumns = map(str, columns)
        strValues = map(self._format, values)
        sql = 'insert into {} ('.format(tableName)
        sql += ','.join(strColumns) + ') values ('
        sql += ','.join(strValues) + ')'
        print('insert sql:' + sql)
        self.update(sql)

    def _format(self, val):
        """
        文字列の前後に'"'(ダブルクオーテーション)がついていなければ追加し、
        datetimeは'2016-10-26 21:11:27'の形にフォーマットし、
        それ以外は文字列に変換する
        """
        import datetime
        if isinstance(val, str):
            ret = val
            if not val.startswith('"'):
                ret = '"' + ret
            if not val.endswith('"'):
                ret += '"'
            return ret
        if isinstance(val, datetime.datetime):
            ret = '"'
            ret += val.strftime('%Y-%m-%d %H:%M:%S')
            ret += '"'
            return ret
        return str(val)

    def commit(self):
        """
        更新処理をデータベースに反映させます
        この処理はclose()に含まれますが、終了処理の前に複数回のコミットを行いたい場合にはこのメソッドを利用してください
        """
        self.connector.commit()
        return self

    def query(self, sql, holder=None):
        """
        SELECT文を実行します
        @return 結果を格納した新しいResultTupleオブジェクト
        """
        if holder is None:
            holder = ()
        self.cursor.execute(sql, holder)
        result = self.cursor.fetchall()  # ディクショナリ(column : value)のタプル
        self.resultTuple = ResultTuple(result)
        self.index = -1
        return self.resultTuple

    def next(self):
        """
        最新のResultTupleオブジェクトのカーソルを次の行に進めます
        """
        return self.resultTuple.next()

    def get(self, column, default=None):
        """
        最新のResultTupleオブジェクトの現在のカーソル行から、指定された列の値を取り出します
        """
        return self.resultTuple.get(column, default)

    def count(self):
        """
        最新結果の行数を返します
        """
        return self.resultTuple.count()

    def reset(self):
        """
        最新結果のカーソルを初期化します
        """
        return self.resultTuple.reset()

    def columns(self, table):
        """
        指定されたテーブルが持つ列名のリストを返します
        ResultTupleのcolumnsとは異なり、順序は保証され、queryの結果に影響を受けません
        """
        self.cursor.execute('desc ' + table)
        result = self.cursor.fetchall()  # ディクショナリ(column : value)のタプル
        ret = []
        for row in result:
            ret.append(row.get('Field'))
        return ret

    def allValues(self, table, column):
        """
        指定されたテーブルが持つ指定された列の値を全て取り出してリストとして返します
        """
        self.cursor.execute('select * from ' + table)
        result = self.cursor.fetchall()
        ret = []
        for row in result:
            ret.append(row.get(column))
        return ret

    def values(self, column=None):
        """
        最新結果のもつ指定された列の値を全て取り出します
        列名が省略された場合は、現在行の値をリストにして返します
        """
        return self.resultTuple.values(column)

    def tables(self):
        """
        テーブル名一覧のリストを返します
        """
        self.cursor.execute('show tables ')
        result = self.cursor.fetchall()  # ディクショナリ(column : value)のタプル
        ret = []
        for row in result:
            ret.append(list(row.values())[0])
        return ret

    def hasTable(self, tableName):
        """
        引数で渡されたテーブルが存在すればTrue、そうでなければFalseを返します
        """
        self.cursor.execute('show tables where Tables_in_{} like %s'.format(self.dbname), (tableName, ))
        if self.cursor.fetchone():
            return True
        else:
            return False

    def createTable(self, tableName, *args):
        """
        [('id', 'int', 'auto_increment', 'not null', 'primary key'), ('name', 'varchar(256), ('registered', 'datetime'), ('memo', 'text')')]
        @param args 挿入する列名、データ型、その他オプションの入ったタプル
        """
        sql = 'create table '
        sql += tableName
        sql += '('
        columnDatas = [' '.join(columnData) for columnData in args]
        sql += ','.join(columnDatas)
        sql += ')'
        self.update(sql)

    def deleteTable(self, tableName):
        """
        指定されたテーブルを削除します
        """
        self.cursor.execute('drop table {}'.format(tableName))


class ResultTuple(object):
    """
    データベースへの問い合わせ結果を取り出すためのクラス
    """

    def __init__(self, resultTuple):
        self.resultTuple = resultTuple
        self.index = -1

    def __iter__(self):
        """
        最新結果の各行の列名から値へのディクショナリをイテレートするイテレータを返します
        このオブジェクトによる元のオブジェクトのカーソル位置への影響はありません
        """
        return self.clone()

    def next(self):
        """
        カーソルを次の行に進めます
        @return 無事にカーソルが進めばtrue
        """
        isNext = self.index < len(self.resultTuple) - 1
        if not isNext:
            return False
        self.index += 1
        return isNext

    def get(self, column, default=None):
        """
        現在行の指定された列の値を取り出します
        @param default 値が存在しなかった場合のデフォルト値
        """
        if not len(self.resultTuple):
            raise RuntimeError('not found column because no selected data')
        if column in self.resultTuple[self.index]:
            return self.resultTuple[self.index][column]
        else:
            return default

    def count(self):
        """
        結果の全行数を返します
        """
        return len(self.resultTuble)

    def reset(self):
        """
        カーソルを初期化します
        """
        self.index = -1
        return self

    def __next__(self):
        if self.next():
            return self.resultTuple[self.index]
        raise StopIteration()

    def clone(self):
        return ResultTuple(self.resultTuple)

    def columns(self):
        """
        列名のリストを返します
        ただし、問い合わせた結果取得したレコードがひとつもない状態で呼び出された際には例外を発生させます
        """
        if not len(self.resultTuple):
            raise RuntimeError('not found column because no data')
        return self.resultTuple[self.index].keys()

    def values(self, column=None):
        """
        指定した列の値をリストにして返します
        列名が省略された場合は、現在行の値をリストにして返します
        """
        if not column:
            return [] if not len(self.resultTuple) else self.resultTuple[self.index].values()
        ret = []
        for row in self.resultTuple:
            ret.append(row[column])
        return ret


# 実行例
if __name__ == '__main__':
    # with文使用可
    """
    select文をquery()に渡せば結果の入ったResultTupleオブジェクトが返ってくる
    """
    with MySql(init_section='tategaki') as obj:

        result = obj.query('select * from edit_users')
        """
        next()でカーソルを次に進めて
        """
        while result.next():
            """
            get()に取得したい列名を渡せばデータが取得できる(データの方はデータベースのデータ型によって決まる)
            """
            print('name =', result.get('name'))
            print('register =', result.get('registered'))  # datetime.datetime
        obj.query('select * from file_table')
        """
        MySqlオブジェクト自体には最後の問い合わせ結果を格納したResultTupleの参照が保持され、直接メソッドを実行することで操作可
        """
        while obj.next():
            print('id =', obj.get('id'))
            print('filename =', obj.get('filename'))
            print('parent_dir =', obj.get('parent_dir'))
        """
        ResultTupleオブジェクトはreset()することでカーソルを初期化して再度利用することもできる
        """
        result.reset()
        while result.next():
            print('name =', result.get('name'))
            print('register =', result.get('registered'))
        result.reset()
        print('---------------- iterator --------------------')
        """
        iteratorは呼び出し元オブジェクトとは別のResultTuple
        よって、for文によって呼び出し元オブジェクトのカーソルは影響を受けない
        keyが列目、valueがその値であるディクショナリがイテレータからは返される
        """
        for dic in result:
            print('dic:', dic)
        for dic in obj:
            print('objDic:', dic)
        """
        呼び出し元オブジェクトはイテレートする前に使い切ってreset()もしてないからここではFalseを返す
        でも直前のfor文では問題なくイテレートされてる
        """
        print(obj.next())  # False
        """
        resultはfor文でイテレートされた直後でまだreset()してないけどイテレート前にreset()してるからTrueを返す
        ちなみに、イテレータで使われる__next__()とnext()は仕様が異なる(前者はカーソルを進めて値を取り出すが、後者はカーソルを進めるだけでbooleanを返す)
        """
        print(result.next())  # True
        """
        取得したレコードの情報を使って列名のリストを作成する。そのためレコードの取得に失敗していればエラー
        """
        print(result.columns())
        """
        MySqlオブジェクトに直接columns()を使った場合、データベースに問い合わせることで列名のリストを作成する
        問い合わせコストはあるかもしれないが、テーブル名さえ間違わなければ安全
        でもいちいちテーブル名を指定しなければいけないから面倒かも
        """
        print(obj.columns('edit_users'))
        """
        引数を指定すると、resultの持っているレコードの全行の値をリストで一気に取得する
        引数を省略するとresultの現在のカーソルの値をリストですべて取得する
        つまり、縦方向にすべての値を取得するか、横方向にすべての値を取得するか、ということ
        """
        print(result.values('name'))
        """
        MySql.values()は最新結果のResultTuple.values()と同じなので、データベースに直接問い合わせて指定列の全行の値を取得したければallValues()を使う
        データベースへの一度の問い合わせでは現在のカーソル行の概念がないため、ここでは縦方向の値に限定される
        """
        print(obj.allValues('file_table', 'filename'))
        """
        テーブル名の一覧も取得できる
        """
        print(obj.tables())

    with MySql() as mysql:
        import datetime
        print(mysql.hasTable('create_sample_table'))
        if not mysql.hasTable('create_sample_table'):
            mysql.createTable(
                'create_sample_table',
                ('id', 'int', 'auto_increment', 'not null', 'primary key', 'unique'),
                ('name', 'varchar(256)', 'not null'),
                ('password', 'varchar(64)'),
                ('registered', 'datetime'),
                ('memo', 'text'),
                ('count', 'int')
            )
            mysql.insert(
                'create_sample_table',
                ('name', 'password', 'registered',
                 'memo', 'count'),
                ('sample', 'password11', datetime.datetime.today(),
                 'insert from database3.4', 25)
            )
            mysql.insert(
                'create_sample_table',
                ('name', 'password', 'registered',
                 'memo', 'count'),
                ('sample2', 'password22', datetime.datetime.today(),
                 'insert from database3.4', 26))
            mysql.delete('create_sample_table', 'name="sample2"')
        else:
            mysql.deleteTable('create_sample_table')
            print(mysql.hasTable('create_sample_table'))

    # new interface
    # db.from(tableName).select(columns..).where('id','=','5').andWhere('score','>','80').orWhere('sex','=','female').execute()
    # where, orWhere
