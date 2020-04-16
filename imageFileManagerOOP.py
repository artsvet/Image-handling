#!/usr/bin/python3

# import pyexiv2
import re
import os
import pathlib
import collections
import datetime
import pandas as pd
import sys
from blackfynn import Blackfynn, Settings
from blackfynn.models import Collection

class ImagePath(type(pathlib.Path())):
    '''
    Base Image class, implementing native Path type
    and basic properties written at acquisition
    '''
    def __init__(self, path):
        super().__init__()

    def get_base_comp(self):
        return re.split(r'[\s,_.]', self.name)

    def get_zstack(self):
        return re.search('z[0-9]+', self.name)[0]

    def get_channel(self):
        channel = re.search('ch[0-9]*', self.name.lower())
        if channel:
            return channel[0]
        else:
            return 'overlay'


class SparcImage(ImagePath):
    '''
    Image class for standard Sparc data format
    Provides interface to read metadata or modify original path.
    Is uploaded to Blackfynn data warehouse
    by passing to BlackfynnUploader.upload_file(SparcImage)
    '''

    def __init__(self, file_path):
        self.file_path = file_path

    def get_base_comp(self):
        sparc_pattern = re.compile(r"[^_]*-[^_.]*", re.IGNORECASE)
        return {key : val for [key, val] in [
            base.split('-') for base in re.findall(sparc_pattern, self.name)
            ]
         }

    def get_sample_id(self):
        return self.get_base_comp()['sam']

    def get_specimen(self):
        return self.get_base_comp()['spec']

    def get_laterality(self):
        return self.get_base_comp()['lat']

    def get_section(self):
        return self.get_base_comp()['sec']

    def get_magnification(self):
        return self.get_base_comp()['mag']

    def get_stain(self):
        return self.get_base_comp()['stain']

    def get_zstack(self):
        try:
            return self.get_base_comp()['z']
        except:
            return ''

    def get_creation_date(self):
        if os.path.isfile(self):
            return str(
                datetime.date.fromtimestamp(
                    os.path.getmtime(
                        self
                    )))
        else:
            print(str(self) + ' is not a real file')
            return ''

    def get_sparc_path(self):
        if self.get_zstack():
            sparc_path = 'samples/sample-{0}/specimen-{1}/laterality-{2}/' \
                         'stain-{3}/section-{4}/magnification-{5}/' \
                         'sam-{0}_spec-{1}_lat-{2}_stain-{3}_sec-{4}_mag-{5}_z-{6}{7}'.format(
                self.get_sample_id(), self.get_specimen(), self.get_laterality(),
                self.get_stain(), self.get_section(), self.get_magnification(),
                self.get_zstack(), self.suffix
            )
        else:
            sparc_path = 'samples/sample-{0}/specimen-{1}/laterality-{2}/' \
                         'stain-{3}/section-{4}/magnification-{5}/' \
                         'sam-{0}_spec-{1}_lat-{2}_stain-{3}_sec-{4}_mag-{5}{6}'.format(
                self.get_sample_id(), self.get_specimen(), self.get_laterality(),
                self.get_stain(), self.get_section(), self.get_magnification(),
                self.suffix
            )
        return SparcImage(sparc_path)

    def get_metadata_dict(self):

        metadata = [self.get_sample_id(), self.get_specimen(), self.get_laterality(),
              self.get_stain(), self.get_section(), self.get_magnification(),
              self.get_zstack(), str(self.get_sparc_path())
              ]

        labels = [
            'sample_id', 'specimen', 'laterality', 'stain',
            'section', 'magnification', 'z_stack', 'sparc_path'

        ]

        if metadata:
            sample_metadata = collections.OrderedDict(zip(labels, metadata))
            return sample_metadata

    def write_xmp(self):
        '''
        Labels image files with metadata values as
        XMP property tags searchable in windows explorer.
        '''
        if self.exists():

            metadata = pyexiv2.ImageMetadata(self)
            metadata.read()
            metadata['Xmp.dc.subject'] = [self.get_sample_id(), self.get_specimen(),
                                          self.get_laterality(), self.get_stain(),
                                          self.get_section(), self.get_magnification(),
                                          self.get_zstack(), self.get_filetype()]
            metadata.write()
        else:
            print(str(self) + ' is not a real file')
            pass

    def rename_to_sparc(self):
        '''
        Renames file at Path
        to Sparc conforming file name
        '''

        if self.exists():
            try:
                return SparcImage(self.rename(self.get_sparc_path().name))

            except Exception as ex:
                print('Rename error for {}.\n{}'.format(self, new_path))
        else:
            print(str(self) + ' is not a real file')

    def write_sparc_path(self, write_to):
        '''
        Replaces Path with Sparc conforming file and
        directory path originating at write_to,
        writes at Path with replacement
        '''

        if self.exists():
            new_path = write_to.joinpath(self.get_sparc_path())
            try:
                return SparcImage(self.replace(new_path))

            except Exception as ex:
                print('Replacement error for {}.\n{}'.format(self, new_path))
        else:
            print(str(self) + ' is not a real file')

    def write_sparc_dir(self, write_to):
        '''
        Writes Sparc conforming directory hierarchy
        originating at write_to
        '''

        new_path = write_to.joinpath(self.get_sparc_path())
        try:
            return new_path.parent.mkDir(parents=True, exist_ok=True)

        except Exception as ex:
            print('Cannot make Sparc directories for {}.\n{}'.format(self, new_path))


    def metadata_to_df(self):
        '''
        Returns Sparc metadata for Path as a pandas Series
        '''
        return pd.Series(self.get_metadata_dict())


class PathFormatFactory:
    '''
    Factory for generating correct Sparc Image
    subclass from base Image Path by checking
    user-generated Path labels
    '''

    def __init__(self, image_path):
        self.image_path = image_path

    def is_sparcy(self):
        sparc_pattern = re.compile(r"sam-\d+_\w+-\w+", re.IGNORECASE)
        if sparc_pattern.search(self.image_path):
            return True

    def format(self):
        if self.is_sparcy:
            return SparcImage(self.image_path)
        if '5ht7' in self.image_path.parts:
            return Ht7(self.image_path)
        if '5ht2a' in self.image_path.parts:
            return Ht2a(self.image_path)
        if '5ht2b' in self.image_path.parts:
            return Ht2b(self.image_path)
        if '5ht' in self.image_path.parts:
            return Ht(self.image_path)
        if 'a2a' in self.image_path.parts:
            return a2a(self.image_path)
        else:
            return self.image_path


class Ht(ImagePath, SparcImage):
    '''
    Subclass of Sparc Image with format
    specific metadata interface
    '''

    def __init__(self, image_path):
        self.image_path = image_path

    def get_sample_id(self):
        return self.get_base_comp()[0]

    def get_laterality(self):
        if self.get_magnification() == '2x':
            return 'whole'
        elif self.get_base_comp()[-4].lower() in ['il', 'l', 'lft']:
            return 'left'
        elif self.get_base_comp()[-4].lower() in ['r', 'cl', 'rt']:
            return 'right'
        else:
            return None

    def get_section(self):
        if self.get_magnification() == '2x':
            return self.get_base_comp()[-4][7:]
        elif 'section' in self.get_base_comp()[1]:
            return self.get_base_comp()[1][7:]
        else:
            return self.get_base_comp()[-5][7:]

    def get_magnification(self):
        return self.get_base_comp()[-3]

    def get_stain(self):
        if self.get_channel() == 'overlay':
            return self.parts[-5]
        else:
            stains = self.parts[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain


class Ht2a(ImagePath, SparcImage):
    '''
    Subclass of Sparc Image with format
    specific metadata interface
    '''

    def __init__(self, file_path):
        self.file_path = file_path

    def get_sample_id(self):
        return self.parts[-3]

    def get_laterality(self):
        return self.parts[-2].split()[1]

    def get_section(self):
        return self.parts[-2].split()[3]

    def get_magnification(self):
        return self.parts[-2].split()[4]

    def get_stain(self):
        if self.get_channel() == 'overlay':
            return self.parts[-5]
        else:
            stains = self.parts[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain


class Ht2b(ImagePath, SparcImage):
    '''
    Subclass of Sparc Image with format
    specific metadata interface
    '''
    def __init__(self, image_path):
        self.image_path = image_path

    def get_sample_id(self):
        return self.parts[-3]

    def get_laterality(self):
        return self.parts[-2].split()[1]

    def get_section(self):
        return self.parts[-2].split()[3]

    def get_magnification(self):
        return self.parts[-2].split()[4]

    def get_stain(self):
        if self.get_channel() == 'overlay':
            return self.parts[-5]
        else:
            stains = self.parts[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain


class Ht7(ImagePath, SparcImage):
    '''
    Subclass of Sparc Image with format
    specific metadata interface
    '''
    def __init__(self, image_path):
        self.image_path = image_path

    def get_sample_id(self):
        return self.parts[-3]

    def get_laterality(self):
        return self.parts[-2].split()[1]

    def get_section(self):
        return self.parts[-2].split()[-2]

    def get_magnification(self):
        return self.parts[-2].split()[-1]

    def get_stain(self):
        if self.get_channel() == 'overlay':
            return self.parts[-5]
        else:
            stains = self.parts[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain


class A2a(ImagePath, SparcImage):
    '''
    Subclass of Sparc Image with format
    specific metadata interface
    '''
    def __init__(self, image_path):
        self.image_path = image_path

    def get_sample_id(self):
        return self.get_base_comp()[2]

    def get_laterality(self):
        return self.get_base_comp()[-3]

    def get_section(self):
        return self.parts[-2].split('_')[3][3:]

    def get_magnification(self):
        return self.parts[-2].split('_')[1]

    def get_stain(self):
        if self.get_channel() == 'overlay':
            return self.parts[-5]
        else:
            stains = self.parts[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain


class BlackfynnUploader:
    '''
    Blackfynn data warehouse interface.
    Initializes using local user profile and
    attempts connection to destination dataset_name.
    Checks/creates Sparc conforming collections
    and uploads file passed to upload_file(to_upload)
    '''

    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.working_profile = self.get_profile()
        self.dataset = self.connect()

    def get_profile(self):
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
                offset = int(working_profile) - 1
                if offset >= 0 and offset < len(list_profile):
                    working_profile = list_profile[offset]
                else:
                    working_profile = ''
        else:
            print('No profiles found, create Blackfynn API profile first.')
            sys.exit()
        return working_profile

    def connect(self):
        '''
        Prompts for csv file with info we need to upload.
        Checks if files to upload exist.
        Connects to Blackfynn API and activates top level dataset destination.
        Returns csv file name, destination dataset.
        '''
        try:
            b_fynn = Blackfynn(self.working_profile)
        except Exception as ex:
            sys.exit('Error Connecting to ' + 'Blackfynn. ' + str(ex))

        try:
            print("Trying to connect to dataset.", flush=True)
            dataset = b_fynn.get_dataset(self.dataset_name)
        except Exception as ex:
            sys.exit('Unable to connect to the dataset.' + str(ex))

        print()
        print('Dataset:  {}\nProfile:  {}'.format(
            data_set, self.working_profile))

        prompt = input('Continue with upload (y/n)? ')
        if prompt != 'y':
            sys.exit('Aborting upload.')
        return dataset

    def make_collection(self, collection, to_upload):
        '''
        Finds or creates one or more collections in a collection hierarchy
        and returns the lowest collection.
        '''
        for level in to_upload.parents:
            for curr_coll in collection.items:
                if curr_coll.name == level:
                    break
            else:
                print('Creating', level, ' in', collection.name)
                curr_coll = collection.create_collection(level)
            collection = curr_coll  # step down into new collection
        return curr_coll

    def check_collection(self, collection, to_upload):
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
                if real_file == to_upload.name:
                    return True
        return False

    def upload_file(self, to_upload):

        if to_upload.exists():

            collection = self.dataset
            print('Uploading {} to {}.'.format(
                to_upload.name, to_upload.get_sparc_path()))

            if self.check_collection(collection, to_upload):
                print('File {} already uploaded to {}.'.format(
                    to_upload.name, to_upload.get_sparc_path()
                ))

            else:
                try:
                    collection = self.make_collection(collection, to_upload)
                    collection.upload(to_upload, display_progress=True)
                except Exception as ex:
                    print('Error uploading {}.  {}'.format(path.name, str(ex)))
                    continue

        else:
            print('Error uploading {}. File does not exist.'.format(path.name))