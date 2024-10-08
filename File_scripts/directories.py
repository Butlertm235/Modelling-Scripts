import os

#Creates new directory if it doesn't exist
def makeNewDir(NewDirName):
    try:
        os.mkdir(NewDirName)
        print('Directory '+NewDirName+' created.')
    except OSError as error:
        print('Directory: '+NewDirName+' already exists. Files may be overwritten.')