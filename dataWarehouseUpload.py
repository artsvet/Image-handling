#!/usr/bin/python3
'''
This program uploads local files to a 3rd party data warehouse using the
Blackfynn API, local profile, and an input csv with source and destination.
Source file and destination collection information must already be formatted to
conform to organization requirements.

Requires Python3, Blackfynn client, and Blackfynn profile.

SPARC-BIDS data formatting standard
https://docs.google.com/presentation/d/1EQPn1FmANpPsFt3CguU-JOQVMMlJsNXluQAK_gb2qVg/edit#slide=id.p1

Blackfynn API reference
https://developer.blackfynn.io/python/latest/index.html

'''

import os
import csv
import glob
import sys
import time
from blackfynn import Blackfynn, Settings
from blackfynn.models import Collection

def checkFilesExist(csv_name):
    '''
    Returns True if all the source files in second column of .csv file exist.
    Returns False if not.
    '''
    okay = True
    print('Making sure that files to upload exist')
    with open(csv_name) as csv_file:
        read = csv.reader(csv_file, delimiter=',')
        next(read)
        files = [fname[1] for fname in read]
        for fname in files:
            match = glob.glob(fname)
            if not match:
                print('The path or file {} does not exist.'.format(fname)
                okay = False
    if okay:
        print('All files in place!')
    return okay

def checkBfynnCollection(collection, fname):
    '''
    Checks to see if the source file name exists in the current collection.
    Drills down into collection to find file.
    '''
    for item in collection:
        if isinstance(item, Collection):
            continue
        true_names = item.sources
        for lookup in true_names:
            real_file = os.path.basename(lookup.s3_key)
            if not real_file:
                print('List of sources empty')
            if real_file == fname:
                return True
    return False

def uploadList(collection, files, name):
    '''
    Uploads list of files to the collection
    '''
    for file_list in files:
        upload = True
        next_file = ''
        for next_file in file_list:
            dest_copy = os.path.basename(next_file)
            if checkBfynnCollection(collection, dest_copy):
                print('File {} already uploaded to {}.'.format(next_file, name))
                upload = False
        if upload:
            print('Uploading', file_list, ' to', name)
            try:
                res = collection.upload(file_list, display_progress=True)
            except Exception as ex:
                print('Error uploading {}.  {}'.format(next_file, str(ex)))
                continue

def makeCollection(collection, paths):
    '''
    Finds or creates one or more collections in a collection hierarchy
    and returns the lowest collection.
    '''
    hier = paths.split('/')
    for level in hier:
        for curr_coll in collection.items:
            if curr_coll.name == level:
                break
        else:
            print('Creating', level, ' in', collection.name)
            curr_coll = collection.create_collection(level)
        collection = curr_coll  # step down into new collection
    return curr_coll

def selectCsv():
    '''
    Show csv files in current dir and get name of one you want.
    If no csv, prompt for path and file.
    '''
    list_csv = glob.glob('*.csv')
    list_csv.sort()
    print()
    print('Select a .csv file')
    working_csv = ''
    if list_csv:
        select = 1
        for cfile in list_csv:
            print('   ', select, cfile)
            select += 1
        working_csv = input('Enter number from list or full path to csv file:')
        if working_csv.isnumeric():
            offset = int(working_csv)-1
            if offset >= 0 and offset < len(list_csv):
                working_csv = list_csv[offset]
    else:
        working_csv = input('Enter input path and csv file name:')
    if working_csv and not os.path.exists(working_csv):
        print('The file {} does not exist.'.format(working_csv))
        working_csv = ''
    return working_csv

def getProfile():
    '''
    Scans local Blackfynn API config, shows profiles, pick one.
    '''
    settings = Settings()
    list_profile = []
    print()
    print('Select a Blackfynn profile')
    for section in settings.config.sections():
        if section not in ['global']:
            list_profile.append(section)
    if list_profile:
        choice = 1
        for prof in list_profile:
            print('   ', choice, prof)
            choice += 1
        working_profile = input('Select profile:')
        if working_profile.isnumeric():
            offset = int(working_profile)-1
            if offset >= 0 and offset < len(list_profile):
                working_profile = list_profile[offset]
            else:
                working_profile = ''
    else:
        print('No profiles found, create Blackfynn API profile first.')
        sys.exit()
    return working_profile

def setup():
    '''
    Prompts for csv file with info we need to upload.
    Checks if files to upload exist.
    Connects to Blackfynn API and activates top level dataset destination.
    Returns csv file name, destination dataset.
    '''
    working_csv = selectCsv()
    if not working_csv:
        sys.exit('No input csv, aborting.')
    if not checkFilesExist(working_csv):
        cont = input('Some files are missing,'
                       'Continue anyway (y/n): ')
        if cont != 'y':
            sys.exit('Uploading aborted.')
    # Profile
    working_profile = getProfile()
    try:
        b_fynn = Blackfynn(working_profile)
    except Exception as ex:
        sys.exit('Error Connecting to ' + 'Blackfynn. ' + str(ex))
    # Dataset
    with open(working_csv) as csvfile:
        in_file = csv.reader(csvfile, delimiter=',')
        header = next(in_file)

    if not working_dset[0] or working_dset[1]:
        sys.exit('No dataset name header')
    try:
        print("Trying to connect to dataset.",flush=True)
        data_set = b_fynn.get_dataset(working_dset)
    except Exception as ex:
        sys.exit('Unable to connect to the dataset.' + str(ex))
    print()
    print('Dataset:  {}\nCSV file: {}\nProfile:  {}'.format(
        data_set.name, working_csv, working_profile))
    prompt = input('Continue with upload (y/n)? ')
    if prompt != 'y':
        sys.exit('Aborting upload.')
    return (working_csv, data_set)

def doUpload(working_csv, data_set):
    '''
    Takes a csv file with top level data set in header, Blackfynn
    collection destination in first column and local source file in second.
    Uploads source file to collection destination in top level data set.
    Steps through collection and creates sub-folders as necessary.
    '''
    curr_data_dir = data_set
    dest_name = data_set.name
    curr_sess_dir = []
    print('Reading file {}'.format(working_csv))
    with open(working_csv) as csvfile:
        in_file = csv.reader(csvfile, delimiter=',')
        top_name = next(in_file)[0]
        for row in in_file:
            dest_folder = row[0]
            src_file = row[1]
            if dest_folder:
                if isinstance(curr_sess_dir, Collection):
                    curr_data_dir = makeCollection(curr_sess_dir, dest_folder)
                else:
                    curr_data_dir = makeCollection(curr_data_dir, dest_folder)
                dest_name = top_level
                if curr_sess_name:
                    dest_name += '/' + curr_sess_name
            if not src_file:
                continue
            expanded_files = makeFileList(src_file)
            if expanded_files:
            uploadList(curr_data_dir, src_file, dest_name)

def main():
    '''
    Takes input csv and uploads to selected dataset on the Blackfynn site.
    '''
    (working_csv, dataset) = setup()
    doUpload(working_csv, dataset)
    print('\nDONE!')

if __name__ == '__main__':
    main()
