#!/usr/bin/python3
'''
This program walks a root directory to find image files and log metadata
properties in a dataframe, with optional command line functions. Data can
then be written to csv log file, used to rename files to conform to SPARC-BIDS
standards, or written to image files for search/filtering in filesystem.

SPARC-BIDS data formatting standard
https://docs.google.com/presentation/d/1EQPn1FmANpPsFt3CguU-JOQVMMlJsNXluQAK_gb2qVg/edit#slide=id.p1
'''

import pyexiv2
import os
import datetime
import pandas as pd
import argparse
import collections
import glob
import csv

def writeXmpTag(file_path, tag_list):
    '''
    Labels image files with metadata values as XMP property tags,
    searchable in windows explorer.
    '''

    metadata = pyexiv2.ImageMetadata(file_path)
    metadata.read()
    metadata['Xmp.dc.subject'] = tag_list
    metadata.write()

def getSampleMetadata(file_path):
    '''
    Blocks of code parse input file path for source-format
    specific metadata. First block is simple test for the SPARC conforming
    format, the rest are user-specific formats. Code outside of blocks assign
    remaining metadata based on common scheme and returns metadata dictionary.
    '''

    try:

        if 'sam-' in os.path.basename(file_path):
            sparc_path = file_path.split('/')[-1]
            subject_id = sparc_path.split('_')[0][4:]
            specimen = sparc_path.split('_')[1].split('-')[1]
            laterality = sparc_path.split('_')[2].split('-')[1]
            section = sparc_path.split('_')[4].split('-')[1]
            magnification = sparc_path.split('_')[5].split('-')[1]
            stain = sparc_path.split('_')[3].split('-')[1]
            z_stack = sparc_path.split('_')[6]
            if '+' in stain:
                channel = 'overlay'
                stain_1 = stain.split('+')[0]
                stain_2 = stain.split('+')[1]
            else:
                channel = 'ch1'
                stain_1 = sparc_path.split('_')[3].split('-')[1]
                stain_2 = None

        elif '5ht2b' in file_path:
            subject_id = file_path.split('/')[-3]
            laterality = file_path.split('/')[-2].split()[1]
            stain_1 = '5ht2b'
            stain_2 = 'ctb'
            section = file_path.split('/')[-2].split()[3]
            magnification = file_path.split('/')[-2].split()[4]

        elif '5ht2a' in file_path:
            subject_id = file_path.split('/')[-3]
            laterality = file_path.split('/')[-2].split()[1]
            stain_1 = '5ht2a'
            stain_2 = 'ctb'
            section = file_path.split('/')[-2].split()[3]
            magnification = file_path.split('/')[-2].split()[4]

        elif '5ht7' in file_path:
            subject_id = file_path.split('/')[-3]
            laterality = file_path.split('/')[-2].split()[1]
            stain_1 = '5ht7'
            stain_2 = 'ctb'
            section = file_path.split('/')[-2].split()[-2]
            magnification = file_path.split('/')[-2].split()[-1]

        elif 'a2a' in file_path:
            subject_id = file_path.split('/')[-1].split('_')[2]
            laterality = file_path.split('/')[-1].split('_')[-3]
            stain_1 = 'a2a'
            stain_2 = 'ctb'
            section = file_path.split('/')[-2].split('_')[3][3:]
            magnification = file_path.split('/')[-2].split('_')[1]

        elif '5ht' in file_path:
            subject_id = file_path.split('/')[-1].split('_')[1]
            magnification = file_path.split('/')[-1].split('_')[-2]
            if 'section' in subject_id:
                subject_id = file_path.split('/')[-1].split('_')[0].split()[2]
            if magnification == '2x':
                laterality = 'whole'
            elif lower(file_path.split('/')[-1].split('_')[-3]) in ['il', 'l', 'lft']:
                laterality = 'left'
            elif lower(file_path.split('/')[-1].split('_')[-3]) in ['r', 'cl', 'rt']:
                laterality = 'right'
            else:
                laterality = None
            stain_1 = '5ht'
            stain_2 = 'ctb'
            if magnification == '2x':
                section = file_path.split('/')[-1].split('_')[-3][7:]
            elif 'section' in file_path.split('/')[-1].split('_')[1]:
                section = file_path.split('/')[-1].split('_')[1][7:]
            else:
                section = file_path.split('/')[-1].split('_')[-4][7:]

        else:
            raise ValueError('Make sure root contains format label')

        specimen = 'phrenic'
        if  'channel' not in locals():
            channel = lower(file_path.split('/')[-1][-7:-4])
        if channel == 'ch1':
            stain = stain_1
            stain_2 = None
        else:
            channel = 'overlay'
            stain = stain_1 + '+' + stain_2
        if  'z_stack' not in locals():
            z_stack = lower(file_path.split('/')[-1].split('_')[-1])
        if 'z0' not in lower(z_stack):
            z_stack = None

        timestamp = str(
                datetime.date.fromtimestamp(
                    os.path.getmtime(
                        file_path
                        )))
        filetype = os.path.splitext(
                file_path
                )[1]

        metadata = [
            timestamp, filetype, subject_id,
            specimen, laterality, stain_1,
            stain_2, channel, stain,
            section, magnification, z_stack
            ]

        labels = [
            'timestamp', 'filetype', 'subject_id',
            'specimen', 'laterality', 'stain_1',
            'stain_2', 'channel', 'stain',
            'section', 'magnification', 'z_stack'
            ]

        if metadata:
            sample_metadata = collections.OrderedDict(zip(labels, metadata))
            return sample_metadata
        else:
            raise ValueError('Metadata missing')

    except Exception as ex:
        print('{} Metadata Error\n{}'.format(file_path, str(ex)))
        traceback.print_tb()
        return {}

def getSparcFilePath(sample_metadata):
    '''
    Takes metadata dictionary and generates a file path conforming to sparc
    data format. Returns sparc file path string.
    '''

    if 'z_stack' in sample_metadata:
        sparc_file_path =
            'samples/sam-{0}/{1}/{2}/{3}/section_{4}/{5}/'
            'sam-{0}_spec-{1}_lat-{2}_stain-{3}_sec-{4}_mag-{5}_{6}_IHC{7}'
            .format(
                sample_metadata['subject_id'], sample_metadata['specimen'],
                sample_metadata['laterality'], sample_metadata['stain'],
                sample_metadata['section'], sample_metadata['magnification'],
                sample_metadata['z_stack'], sample_metadata['filetype'],
                )

    else:
        sparc_file_path =
            'samples/sam-{0}/{1}/{2}/{3}/section_{4}/{5}/'
            'sam-{0}_spec-{1}_lat-{2}_stain-{3}_sec-{4}_mag-{5}_IHC{6}'
            .format(
                sample_metadata['subject_id'], sample_metadata['specimen'],
                sample_metadata['laterality'], sample_metadata['stain'],
                sample_metadata['section'], sample_metadata['magnification'],
                sample_metadata['filetype'],
                )

    return sparc_file_path

def changeBaseName(file_path, sparc_file_path):
    '''
    Renames base name at file_path to conform to sparc_file_path.
    '''

    if os.path.basename(file_path) == os.path.basename(sparc_file_path):
        print('{} already in sparc format'.format(file_path))
        continue
    else:
        try:
            os.rename(file_path, file_path.replace(
                os.path.basename(file_path), os.path.basename(sparc_file_path)
                )
                    )
        except Exception as ex:
            print('Rename error for {}. {}'.format(file_path, str(ex)))
            continue

def collectDataframe(to_walk, dfSamples):
    '''
    Walks directory tree starting in to_walk, gets metadata and formatted file
    path for .tif image files and collects them as rows in a dataframe.
    Returns full dataframe.
    '''

    for root, dirs, files in os.walk(to_walk):
        for file_path in files:
            file_path = root + '/' + file_path
            if file_path.endswith('.tif'):
                sample_metadata = getSampleMetadata(file_path)
                if sample_metadata:
                    sparc_file_path = getSparcFilePath(sample_metadata)
                    sample_metadata['sparc_file_path'] = sparc_file_path
                sample_metadata['current_file_path'] = file_path
                df = pd.DataFrame(data=sample_metadata,
                                       columns=sample_metadata.keys(),index=[0])
                dfSamples = dfSamples.append(df, ignore_index=True)
            else:
                pass
    return dfSamples

def writeMetadata(fromDf, toCsv):
    '''
    Writes metadata from collected dataframe to a log csv files.
    '''
    with open(toCsv, 'a+') as f:
        has_data = f.readline()
        if has_data:
            f.to_csv(
                f, header=False, index=False, line_terminator="\n"
                )
        else:
            f.to_csv(
                f, index=False, line_terminator="\n"
                )

def parseArguments():
    '''
    Parses command line arguments for optional file management
    functions, source and destination file names. Returns arguments object.
    '''

    parser = argparse.ArgumentParser(description='Provide directory path to'
    'walk and optionally write metadata, change names, or tag files.')
    parser.add_argument('-wd', '--working_dir', type=str, default=getcwd()
                        help='set directory path to parse for files, defaults '
                        'to current working directory.')
    parser.add_argument('-cn', '--change_name',
                        help='set to rename found files to sparc format',
                        action="store_true")
    parser.add_argument('-mf', '--metadata_file', type=str,
                        help='set with valid file path to write metadata')
    parser.add_argument('-tag', '--write_tags',
                        help='set to write metadata to files in xmp namespace',
                        action="store_true")
    args = parser.parse_args()

    dir = glob.glob(args.working_dir)
    if not dir:
        print('Invalid directory specified in arguments.')
        exit()

    if args.metadata_file and not args.metadata_file.endswith('.csv'):
        print('Invalid file name for writing metadata')
        exit()

    return args

def setup():
    '''
    Sets up command line arguments, checks/creates Csv file to write metadata,
    initializes data frame. Returns arguments and dataframe objects.
    '''
    args = parseArguments()
    if args.metadata_file:
        try:
            with open(args.metadata_file, 'a+') as csv:
                csv.close()
        except Exception as ex:
            print('Error creating metadata .csv file. {}'.format(str(ex)))
            exit()
    dFrame = pd.DataFrame(data=None, columns=None)
    return(args, dFrame)

def main():
    '''
    Collects metadata dataframe and executes optional log csv file, file
    renaming, and xmp metadata file tagging functions. Prints collected
    dataframe header.
    '''
    (args, dFrame) = setup()
    dFrame = collectDataframe(args.working_dir, dFrame)
    if args.metadata_file:
        writeMetadata(dFrame, args.metadata_file)
    if args.change_name:
        for row in dFrame.iterrows():
            changeBaseName(row[1]['current_file_path'],
                           row[1]['sparc_File_Path'])
    if args.write_tags:
        tagLabels = ['subject_id', 'specimen', 'laterality', 'stain_1',
                    'stain_2', 'channel', 'section', 'magnification']
        for row in dFrame.iterrows():
            tagList = []
            for label in tagLabels:
                try:
                    tagList.append(row[1][label])
                except KeyError:
                    print('Missing metadata {}\n{}'.format(
                          label,row[1]['current_file_path']))
            writeXmpTag(row[1]['current_file_path'], filter(None,tagList))]

    print(dFrame.head())


if __name__ == '__main__':
    main()
