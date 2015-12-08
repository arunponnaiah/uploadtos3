#! /usr/bin/python

import tinys3,sys,glob,os,datetime,logging,logging.handlers

# variables
DIR = sys.argv[1]
S3_PATH = sys.argv[2]
AWS_ACCESS_KEY='AKIAJFMNNUBCUK7WDGGA'
AWS_SECRET_KEY='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'
FILE_TYPE='*.csv.gz'
my_logger = logging.getLogger('MyLogger')

#logging
def initLogger(dir):
 if not os.path.exists(dir):
    os.mkdir(dir)
 LOG_FILENAME='./logs/uploadtos3.log'
 logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
 		    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=LOG_FILENAME,
                    filemode='w')
 # Add the log message handler to the logger
 handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=1048576, backupCount=5)
 my_logger.addHandler(handler)


# get AWS directory with current date
def getDatedS3directory():
 now = datetime.datetime.now()
 date = now.strftime("%Y-%m-%d") 
 return S3_PATH+date

# file upload process
def upload():
 try:
  conn = tinys3.Connection(AWS_ACCESS_KEY,AWS_SECRET_KEY,tls=True)
  os.chdir(DIR)
  S3_PATH_WITH_DATE=getDatedS3directory()
  my_logger.debug( 'S3_PATH_WITH_DATE : %s' % S3_PATH_WITH_DATE)
  for filename in glob.glob(FILE_TYPE):
   f = open(filename,'rb')
   conn.upload(filename,f, S3_PATH_WITH_DATE)
   my_logger.debug('Uploaded :%s' % filename)
  my_logger.debug('Successfully uploaded all files from %s' % DIR)
  delete()
 except Exception as e:
  my_logger.error('Upload failed : %s' % e)

# file delete process
def delete():
 try:
  os.chdir(DIR)
  for filename in glob.glob(FILE_TYPE):
   os.remove(filename)
   my_logger.debug('Deleted : %s' % filename)
  my_logger.debug('Successfully deleted all files from %S' % DIR)
 except Exception as e:
  my_logger.error('Delete failed :%s' %e)

def main():
  initLogger('./logs/')
  upload()

main()
