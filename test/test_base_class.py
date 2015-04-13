import pytest
import ConfigParser


class TestBaseClass(object):
    hostlist = []
    user = None
    password = None

    @staticmethod
    def get_hosts():
        config = ConfigParser.ConfigParser()
        config.read("config.conf")
        sections = config.sections()
        for section in sections:
            options = config.options(section)
            for option in options:
                optionvalue = config.get(section, option)
                if option == "user":
                    if optionvalue != 'None':
                        TestBaseClass.user = optionvalue.strip('\"')
                        continue
                if option == "password":
                    if optionvalue != 'None':
                        TestBaseClass.password = optionvalue.strip('\"')
                        continue
                semicount = optionvalue.count(';')
                for i in xrange(semicount):
                    tup = ()
                    semicolonindex = optionvalue.find(';')
                    index = optionvalue.find(':')
                    tup += (optionvalue[:index], )
                    substr = optionvalue[index + 1:semicolonindex]
                    tup += (int(substr), )
                    optionvalue = optionvalue[semicolonindex + 1:]
                    TestBaseClass.hostlist.append(tup)
        return TestBaseClass.hostlist, TestBaseClass.user, TestBaseClass.password
