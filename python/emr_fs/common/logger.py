import logging.config
import logging, logging.handlers, time, os

class Log(object):

  def __init__(self,type):
    # 定义对应的程序模块名name，默认为root
    self.logger = logging.getLogger()
    work_dir = os.path.dirname(os.path.abspath(__file__))
    print(work_dir)
    CONF_FILE = os.path.join(work_dir,'./logging.conf')
    logconf=open(CONF_FILE,encoding='UTF-8')
    logging.config.fileConfig(logconf)
    logconf.close()
    if type=='file':
       self.logger = logging.getLogger('file')
    else :
       self.logger = logging.getLogger("console")


  def info(self, message):
        self.logger.info(message)

  def debug(self, message):
        self.logger.debug(message)

  def warning(self, message):
        self.logger.warning(message)

  def error(self, message):
        self.logger.error(message)

  def critical(self, message):
        self.logger.critical(message)
