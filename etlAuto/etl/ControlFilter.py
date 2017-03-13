# -*- coding: utf-8 -*-
import os
import sys
import re
import datetime
import time
import logging

class ControlFilter:
    ## 由于python不具备const常量定义功能，暂时先不实现const变量功能
    RCV_FLAG = 1
    MASTER_FLAG =2
    MESSAGE_FLAG = 3
    LOG_FLAG = 4
    rcv_master_flag = 0
    pattern = ""

    def __init__(self,mode):
        self.rcv_master_flag = mode

    def getPattern(self):
        if(self.rcv_master_flag == self.RCV_FLAG):
            self.pattern = "dir.*"
        if(self.rcv_master_flag == self.MASTER_FLAG):
            self.pattern = "*.DIR"
        if(self.rcv_master_flag == self.MESSAGE_FLAG):
            self.pattern = "*.msg"
        if(self.rcv_master_flag == self.LOG_FLAG):
            self.pattern = "*.log"

        return self.pattern        

if __name__ == "__main__":
    ctrlFilter = ControlFilter(4)
    print ctrlFilter.getPattern()

 
