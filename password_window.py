#! /usr/bin/python3.4
# -*- coding: utf-8 -*-

import sys
import os
from database3_4 import MySql

from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QHBoxLayout
from PyQt5.QtWidgets import QLabel, QLineEdit, QComboBox
from PyQt5.QtWidgets import QVBoxLayout, QTextEdit, QFormLayout

INIT_SECTION = 'password'
INIT_FILE = os.path.join(os.environ.get('HOME'), 'python/PyPassword/config.ini')
PASSWORD = 'digk473'


class Searchable:
    """
    ツリー構造になっている各要素のオブジェクトを取得するためのクラスです
    """

    def __init__(self):
        self.children = []
        self._parent = None

    def add(self, child):
        self.children.append(child)
        child._parent = self

    def findAll(self, selector):
        """
        自分とその子孫要素に関数を適用し、Trueとなった要素がすべて入ったリストを作成します
        @param selector 要素をひとつ引数に取り、真偽値を返す関数
        """
        ret = []
        if selector(self):
            ret.append(self)
        for child in self.children:
            ret.extend(child._findAll())
        return ret

    def findElem(self, selector):
        """
        渡された関数に子孫要素を引数に渡し、最初にTrueとなった要素を返します
        """
        for elem in self._findNext():
            if selector(elem):
                return elem
        return None

    def _findNext(self):
        """
        自分とその子孫要素を次々と返していくジェネレータです
        """
        yield self
        for child in self.children:
            for ret in child._findNext():
                yield ret
        raise StopIteration

    def root(self):
        elem = self
        while elem._parent:
            elem = elem._parent
        return elem


class NameLabel(QLabel, Searchable):

    def __init__(self):
        QLabel.__init__(self, 'Name')
        Searchable.__init__(self)


class NameInput(QLineEdit, Searchable):

    def __init__(self):
        QLineEdit.__init__(self)
        Searchable.__init__(self)


class NameLayout(QVBoxLayout, Searchable):

    def __init__(self):
        QVBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        label = NameLabel()
        self.add(label)
        self.addWidget(label)
        edit = NameInput()
        self.add(edit)
        self.addWidget(edit)


class PasswordLabel(QLabel, Searchable):

    def __init__(self):
        QLabel.__init__(self, 'password')
        Searchable.__init__(self)


class PasswordInput(QLineEdit, Searchable):

    def __init__(self):
        QLineEdit.__init__(self)
        Searchable.__init__(self)


class PasswordLayout(QVBoxLayout, Searchable):

    def __init__(self):
        QVBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        label = PasswordLabel()
        self.add(label)
        self.addWidget(label)
        edit = PasswordInput()
        self.add(edit)
        self.addWidget(edit)


class MemoLabel(QLabel, Searchable):

    def __init__(self):
        QLabel.__init__(self, 'memo')
        Searchable.__init__(self)


class MemoInput(QTextEdit, Searchable):

    def __init__(self):
        QTextEdit.__init__(self, '')
        Searchable.__init__(self)


class MemoLayout(QVBoxLayout, Searchable):

    def __init__(self):
        QVBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        label = MemoLabel()
        self.add(label)
        self.addWidget(label)
        edit = MemoInput()
        self.add(edit)
        self.addWidget(edit)


class InputFormLayout(QFormLayout, Searchable):

    def __init__(self):
        QFormLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        self.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        name = NameLayout()
        self.add(name)
        self.addRow(name)
        password = PasswordLayout()
        self.add(password)
        self.addRow(password)
        memo = MemoLayout()
        self.add(memo)
        self.addRow(memo)


class NewButton(QPushButton, Searchable):
    style = '''
    min-height: 1.5em;
    '''

    def __init__(self):
        QPushButton.__init__(self, 'new')
        Searchable.__init__(self)
        self.clicked.connect(self._click)
        self.setStyleSheet(self.style)

    def _click(self):
        selectCombo = self.root().findElem(
            lambda e: isinstance(e, SelectCombo))
        selectCombo.setCurrentIndex(0)


class SelectComboLabel(QLabel, Searchable):

    def __init__(self):
        QLabel.__init__(self, 'select')
        Searchable.__init__(self)


class SelectCombo(QComboBox, Searchable):
    style = '''
    SelectCombo {
        font: 20px;
        min-height: 1.5em;
        }
    '''

    def __init__(self):
        QComboBox.__init__(self)
        Searchable.__init__(self)
        self._addNames()
        self.currentIndexChanged.connect(self._changedText)
        # self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setStyleSheet(self.style)

    def _addNames(self):
        with MySql(passwd=PASSWORD, init_file=INIT_FILE, init_section=INIT_SECTION) as mysql:
            self.addItem('new')
            names = mysql.allValues('password_table', 'name')
            for name in names:
                self.addItem(name)

    def _changedText(self):
        selectedText = self.currentText()
        selectIndex = self.currentIndex()
        nameInput = self.root().findElem(
            lambda e: isinstance(e, NameInput))
        passwordInput = self.root().findElem(
            lambda e: isinstance(e, PasswordInput))
        memoInput = self.root().findElem(
            lambda e: isinstance(e, MemoInput))
        if selectIndex == 0:
            nameInput.setText('')
            passwordInput.setText('')
            memoInput.setText('')
        else:
            with MySql(passwd=PASSWORD, init_file=INIT_FILE, init_section=INIT_SECTION) as mysql:
                result = mysql.query(
                    'select * from password_table where name=%s',
                    (selectedText,))
                try:
                    nameInput.setText(result.get('name'))
                    passwordInput.setText(result.get('password', ''))
                    memoInput.setText(result.get('memo', ''))
                except RuntimeError:
                    """
                    データの新規作成及び更新を行うとコンボボックス内の選択状態を変える必
                    要が出てくるが、プログラム上での選択状態変更でもイベントは発生する。
                    しかし、データベースの更新は非同期であるのか、更新直後で問い合わせを行って
                    も新しい名前のレコードが存在せずRuntimeErrorが発生する。
                    よって、更新削除後の UI変更はプログラム上ですべて行うこととし、エラーが発生
                    してもエラー処理を行わず、このまま続行する

                    削除の場合はエラーが出ないのはなぜ？
                    = 削除後は他の(=削除行為以前から存在している)データを取り出すことになるため
                    """
                    pass


class SelectComboLayout(QHBoxLayout, Searchable):

    def __init__(self):
        QHBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        label = SelectComboLabel()
        self.add(label)
        self.addWidget(label)
        combo = SelectCombo()
        self.add(combo)
        self.addWidget(combo)


class DummyWidget(QWidget, Searchable):
    style = '''
    '''

    def __init__(self):
        QWidget.__init__(self)
        Searchable.__init__(self)
        self.setStyleSheet(self.style)


class LefterLayout(QVBoxLayout, Searchable):

    def __init__(self):
        QVBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        newButton = NewButton()
        self.add(newButton)
        self.addWidget(newButton)
        name = SelectComboLayout()
        self.add(name)
        self.addLayout(name)
        dummy = DummyWidget()
        self.add(dummy)
        self.addWidget(dummy)


class UpperLayout(QHBoxLayout, Searchable):

    def __init__(self):
        QHBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        lefter = LefterLayout()
        self.add(lefter)
        self.addLayout(lefter)
        inputForm = InputFormLayout()
        self.add(inputForm)
        self.addLayout(inputForm)


class RegistButton(QPushButton, Searchable):
    """
    登録・更新ボタン
    """

    def __init__(self):
        QPushButton.__init__(self, 'regist/update')
        Searchable.__init__(self)
        self.clicked.connect(self._click)

    def _click(self):
        import datetime
        selectCombo = self.root().findElem(
            lambda e: isinstance(e, SelectCombo))
        selectedText = selectCombo.currentText()
        nameInput = self.root().findElem(
            lambda e: isinstance(e, NameInput))
        passwordInput = self.root().findElem(
            lambda e: isinstance(e, PasswordInput))
        memoInput = self.root().findElem(
            lambda e: isinstance(e, MemoInput))
        newName = nameInput.text()
        newPassword = passwordInput.text()
        newMemo = memoInput.toPlainText()
        newLatestUpdate = datetime.datetime.today()
        selectIndex = selectCombo.currentIndex()
        if len(newName) == 0:
            print('empty name -> return')
            return  # 名前欄が空欄なら何もしない
        with MySql(passwd=PASSWORD, init_file=INIT_FILE, init_section=INIT_SECTION) as mysql:
            if selectIndex == 0:
                # 新規作成
                mysql.insert(
                    'password_table',
                    ('name', 'password', 'memo',
                     'created', 'latest_update'),
                    (newName, newPassword, newMemo,
                     newLatestUpdate, newLatestUpdate)
                )
                # combobox上のデータ更新
                selectCombo.addItem(newName)
                selectCombo.setCurrentIndex(selectCombo.count() - 1)
            else:
                # データ更新
                mysql.updateSet(
                    'password_table',
                    ('name', 'password', 'memo', 'latest_update'),
                    (newName, newPassword, newMemo, newLatestUpdate),
                    'name="{}"'.format(selectedText)
                )
                # combobox上の名前を更新
                selectCombo.setItemText(selectIndex, newName)


class DeleteButton(QPushButton, Searchable):

    def __init__(self):
        QPushButton.__init__(self, 'delete')
        Searchable.__init__(self)
        self.clicked.connect(self._click)

    def _click(self):
        selectCombo = self.root().findElem(
            lambda e: isinstance(e, SelectCombo))
        selectedText = selectCombo.currentText()
        selectIndex = selectCombo.currentIndex()
        if selectIndex == 0:
            return
        with MySql(passwd=PASSWORD, init_file=INIT_FILE, init_section=INIT_SECTION) as mysql:
            mysql.delete('password_table', 'name="{}"'.format(selectedText))
            selectCombo.removeItem(selectIndex)


class ButtonLayout(QHBoxLayout, Searchable):

    def __init__(self):
        QHBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        regist = RegistButton()
        self.add(regist)
        self.addWidget(regist)
        delete = DeleteButton()
        self.add(delete)
        self.addWidget(delete)


class MainLayout(QVBoxLayout, Searchable):

    def __init__(self):
        QVBoxLayout.__init__(self)
        Searchable.__init__(self)
        self._initUI()

    def _initUI(self):
        upper = UpperLayout()
        self.add(upper)
        self.addLayout(upper)
        button = ButtonLayout()
        self.add(button)
        self.addLayout(button)


class PasswordUIWindow(QWidget, Searchable):

    def __init__(self):
        QWidget.__init__(self)
        Searchable.__init__(self)
        self._createTable()
        self._initUI()

    def _initUI(self):
        self.setWindowTitle('password keeper')
        main = MainLayout()
        self.add(main)
        self.setLayout(main)

    def _createTable(self):
        with MySql(passwd=PASSWORD, init_file=INIT_FILE, init_section=INIT_SECTION) as mysql:
            if not mysql.hasTable('password_table'):
                mysql.createTable(
                    'password_table',
                    ('id', 'int', 'auto_increment', 'not null', 'primary key'),
                    ('name', 'varchar(255)', 'unique', 'not null'),
                    ('password', 'varchar(64)'),
                    ('memo', 'text'),
                    ('created', 'datetime'),
                    ('latest_update', 'datetime')
                )


if __name__ == '__main__':

    app = QApplication(sys.argv)

    window = PasswordUIWindow()

    window.show()

    sys.exit(app.exec_())
