# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime
import logging
from ETL import ETL

logging.basicConfig(level=logging.INFO)

class ControlFileCompare:
    def compare(self,a,b):
        date1 = a[len(a)-8:len(a)]
        date2 = b[len(b)-8:len(b)]
        logging.info(date1+" and "+date2)
        result = cmp(date1,date2)
        return result

if __name__ == '__main__':
    a = 'xxx.20170819'
    b = 'xxx.20170820'
    compare = ControlFileCompare()
    print compare.compare(a,b)
