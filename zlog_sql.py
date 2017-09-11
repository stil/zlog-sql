import inspect
import os
import pprint
import re
import sqlite3
import traceback
from datetime import datetime

import pymysql
import znc


# noinspection PyPep8Naming
class zlog_sql(znc.Module):
    module_types = [znc.CModInfo.GlobalModule]
    description = 'Log all channels to a MySQL/SQLite database.'
    has_args = True
    args_help_text = 'Connection string in format: mysql://user:pass@host/database_name or sqlite://path/to/db.sqlite'
    hook_debugging = False

    def OnLoad(self, args, message):
        """
        This module hook is called when a module is loaded.
        :type args: const CString &
        :type args: CString &
        :rtype: bool
        :param args: The arguments for the modules.
        :param message: A message that may be displayed to the user after loading the module.
        :return: True if the module loaded successfully, else False.
        """
        self.debug_hook()
        try:
            self.db = TargetDatabase(args)
            return True
        except Exception as e:
            message.s = str(e)
            return False

    def GetServer(self):
        pServer = self.GetNetwork().GetCurrentServer()

        if pServer is None:
            return '(no server)'

        if pServer.IsSSL():
            sSSL = "+"
        else:
            sSSL = ""

        return pServer.GetName() + ' ' + sSSL + pServer.GetPort()

    def OnIRCConnected(self):
        """
        This module hook is called after a successful login to IRC.
        :rtype: None
        """
        self.debug_hook()
        self.put_log('Connected to IRC (' + self.GetServer() + ')')

    def OnIRCDisconnected(self):
        """
        This module hook is called when a user gets disconnected from IRC.
        :rtype: None
        """
        self.debug_hook()
        self.put_log('Disconnected from IRC (' + self.GetServer() + ')')

    def OnBroadcast(self, message):
        """
        This module hook is called when a message is broadcasted to all users.
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('Broadcast: ' + str(message))
        return znc.CONTINUE

    def OnRawMode(self, opNick, channel, modes, args):
        """
        Called on any channel mode change.
        This is called before the more detailed mode hooks like e.g. OnOp() and OnMode().
        :type opNick: const CNick &
        :type channel: CChan &
        :type modes: const CString &
        :type args: const CString &
        :rtype: None
        """
        self.debug_hook()
        sNick = opNick.GetNick() if opNick is not None else 'Server'
        self.put_log('*** ' + sNick + ' sets mode: ' + modes + ' ' + args, channel.GetName())

    def OnKick(self, opNick, kickedNick, channel, message):
        """
        Called when a nick is kicked from a channel.
        :type opNick: const CNick &
        :type kickedNick: const CString &
        :type channel: CChan &
        :type message: const CString &
        :rtype: None
        """
        self.debug_hook()
        self.put_log('*** ' + kickedNick + ' was kicked by ' + opNick.GetNick() + ' (' + message + ')',
                     channel.GetName())

    def OnQuit(self, nick, message, channels):
        """
        Called when a nick quit from IRC.
        :type nick: const CNick &
        :type message: const CString &
        :type channels: std::vector<CChan*>
        :rtype: None
        """
        self.debug_hook()
        for channel in channels:
            self.put_log(
                '*** Quits: ' + nick.GetNick() + ' (' + nick.GetIdent() + '@' + nick.GetHost() + ') (' + message + ')',
                channel.GetName())

    def OnJoin(self, nick, channel):
        """
        Called when a nick joins a channel.
        :type nick: const CNick &
        :type channel: CChan &
        :rtype: None
        """
        self.debug_hook()
        self.put_log('*** Joins: ' + nick.GetNick() + ' (' + nick.GetIdent() + '@' + nick.GetHost() + ')',
                     channel.GetName())

    def OnPart(self, nick, channel, message):
        """
        Called when a nick parts a channel.
        :type nick: const CNick &
        :type channel: CChan &
        :type message: const CString &
        :rtype: None
        """
        self.debug_hook()
        self.put_log(
            '*** Parts: ' + nick.GetNick() + ' (' + nick.GetIdent() + '@' + nick.GetHost() + ') (' + message + ')',
            channel.GetName())

    def OnNick(self, oldNick, newNick, channels):
        """
        Called when a nickname change occurs.
        :type oldNick: const CNick &
        :type newNick: const CString &
        :type channels: std::vector<CChan*>
        :rtype: None
        """
        self.debug_hook()
        for channel in channels:
            self.put_log('*** ' + oldNick.GetNick() + ' is now known as ' + newNick, channel.GetName())

    def OnTopic(self, nick, channel, topic):
        """
        Called when we receive a channel topic change from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type topic: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('*** ' + nick.GetNick() + ' changes topic to "' + str(topic) + '"', channel.GetName())
        return znc.CONTINUE

    # NOTICES
    # =======

    def OnUserNotice(self, target, message):
        """
        This module hook is called when a user sends a NOTICE message.
        :type target: CString &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        network = self.GetNetwork()
        if network:
            self.put_log('-' + network.GetCurNick() + '- ' + str(message), str(target))

        return znc.CONTINUE

    def OnPrivNotice(self, nick, message):
        """
        Called when we receive a private NOTICE message from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('-' + nick.GetNick() + '- ' + str(message), nick.GetNick())
        return znc.CONTINUE

    def OnChanNotice(self, nick, channel, message):
        """
        Called when we receive a channel NOTICE message from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('-' + nick.GetNick() + '- ' + str(message), channel.GetName())
        return znc.CONTINUE

    # ACTIONS
    # =======

    def OnUserAction(self, target, message):
        """
        Called when a client sends a CTCP ACTION request ("/me").
        :type target: CString &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        pNetwork = self.GetNetwork()
        if pNetwork:
            self.put_log('* ' + pNetwork.GetCurNick() + ' ' + str(message), str(target))

        return znc.CONTINUE

    def OnPrivAction(self, nick, message):
        """
        Called when we receive a private CTCP ACTION ("/me" in query) from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('* ' + nick.GetNick() + ' ' + str(message), nick.GetNick())
        return znc.CONTINUE

    def OnChanAction(self, nick, channel, message):
        """
        Called when we receive a channel CTCP ACTION ("/me" in a channel) from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('* ' + nick.GetNick() + ' ' + str(message), channel.GetName())
        return znc.CONTINUE

    # MESSAGES
    # ========

    def OnUserMsg(self, target, message):
        """
        This module hook is called when a user sends a PRIVMSG message.
        :type target: CString &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        network = self.GetNetwork()
        if network:
            self.put_log('<' + network.GetCurNick() + '> ' + str(message), str(target))

        return znc.CONTINUE

    def OnPrivMsg(self, nick, message):
        """
        Called when we receive a private PRIVMSG message from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('<' + nick.GetNick() + '> ' + str(message), nick.GetNick())
        return znc.CONTINUE

    def OnChanMsg(self, nick, channel, message):
        """
        Called when we receive a channel PRIVMSG message from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log('<' + nick.GetNick() + '> ' + str(message), channel.GetName())
        return znc.CONTINUE

    # LOGGING
    # =======

    def put_log(self, line, window="Status"):
        try:
            self.db.insert_into('logs', {
                'created_at': datetime.utcnow().isoformat(),
                'user': self.GetUser().GetUserName() if self.GetUser() is not None else None,
                'network': self.GetNetwork().GetName() if self.GetUser() is not None else None,
                'window': window,
                'message': line})

        except Exception as e:
            with open(os.path.join(self.GetSavePath(), 'error.log'), 'a') as file:
                file.write('Could not save to database caused by: {0} {1}\n'.format(type(e), str(e)))
                file.write('Stack trace: ' + traceback.format_exc())
                file.write('\n')

    # DEBUGGING
    # =========

    def debug_hook(self):
        """
        Dumps parent calling method name and its arguments to debug logfile.
        """

        if self.hook_debugging is not True:
            return

        frameinfo = inspect.stack()[1]
        argvals = frameinfo.frame.f_locals

        with open(os.path.join(self.GetSavePath(), 'debug.log'), 'a') as file:
            file.write('Called method: ' + frameinfo.function + '()\n')
            for argname in argvals:
                if argname == 'self':
                    continue
                file.write('    ' + argname + ' -> ' + pprint.pformat(argvals[argname]) + '\n')
            file.write('\n')


class TargetDatabase:
    def __init__(self, dsn):

        match = re.search('mysql://(.+?):(.+?)@(.+?)/(.+)', dsn)
        if match:
            self.type = 'mysql'
            self.conn = pymysql.connect(host=match.group(3),
                                        user=match.group(1),
                                        passwd=match.group(2),
                                        db=match.group(4),
                                        use_unicode=True,
                                        charset='utf8mb4')
            return

        match = re.search('sqlite://(.+)', dsn)
        if match:
            self.type = 'sqlite'
            self.conn = sqlite3.connect(match.group(1))
            return

        raise Exception('Unsupported database type. Supported: mysql, sqlite.')

    def insert_into(self, table, row):
        if self.type == 'mysql':
            self.mysql_insert_into(self.conn, table, row)
        elif self.type == 'sqlite':
            self.sqlite_insert_into(self.conn, table, row)
        else:
            raise Exception('Unsupported db type {0}'.format(self.type))

    @staticmethod
    def mysql_insert_into(conn, table, row):
        cols = ', '.join('`{}`'.format(col) for col in row.keys())
        vals = ', '.join('%({})s'.format(col) for col in row.keys())
        sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(table, cols, vals)
        conn.cursor().execute(sql, row)
        conn.commit()

    @staticmethod
    def sqlite_insert_into(conn, table, row):
        cols = ', '.join('[{}]'.format(col) for col in row.keys())
        vals = ', '.join(':{}'.format(col) for col in row.keys())
        sql = 'INSERT INTO [{}] ({}) VALUES ({})'.format(table, cols, vals)
        conn.cursor().execute(sql, row)
        conn.commit()
