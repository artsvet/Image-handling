#!/usr/bin/python3

import os
import pyexiv2
import datetime

'''
to do:
@ decorators
Filter for input to fit format
mock-up in jupyter notebook        
   
'''

class SparcImage:

    '''is formatted to fit standard'''
    def __init__(
            self, file_path, *args, **kwargs
            ):
        self.file_path = file_path

    def get_subject_id(self):
        return self.file_path.split('_')[0].split('-')[2]

    def get_specimen(self):
        return self.file_path.split('_')[1].split('-')[1]

    def get_laterality(self):
        return self.file_path.split('_')[2].split('-')[1]

    def get_section(self):
        return self.file_path.split('_')[4].split('-')[1]
          
    def get_magnification(self):
        return self.file_path.split('_')[5].split('-')[1]

    def get_stain(self):
        return self.file_path.split('_')[3].split('-')[1]

    def get_zstack(self):
        z_stack = os.path.splitext(self.file_path)[0].lower().split('_')[-1]
        if 'z[0-9]' in z_stack:
            return z_stack
        else:
            return None

    def get_filetype(self):
        return os.path.splitext(self.file_path)[1]

    def get_creation_date(self):
        return str(
                datetime.date.fromtimestamp(
                    os.path.getmtime(
                        self.file_path
                        )))

    def test_methods(self):
        print(self.get_subject_id(), self.get_specimen(), self.get_laterality(),
            self.get_stain(), self.get_section(), self.get_magnification(),
            self.get_zstack(), self.get_filetype()
            )

    def write_xmp(self):
        '''
        Labels image files with metadata values as XMP property tags,
        searchable in windows explorer.
        '''

        metadata = pyexiv2.ImageMetadata(self.file_path)
        metadata.read()
        metadata['Xmp.dc.subject'] = [self.get_subject_id(), self.get_specimen(),
                                      self.get_laterality(), self.get_stain(),
                                      self.get_section(), self.get_magnification(),
                                      self.get_zstack(), self.get_filetype()]
        metadata.write()

    def get_sparc_path(self):
        if self.get_zstack():
            sparc_path = 'samples/sam-{0}/{1}/{2}/{3}/{4}/{5}/'\
            'sam-{0}_spec-{1}_lat-{2}_stain-{3}_sec-{4}_mag-{5}_{6}{7}'.format(
            self.get_subject_id(), self.get_specimen(), self.get_laterality(),
            self.get_stain(), self.get_section(), self.get_magnification(),
            self.get_zstack(), self.get_filetype()
            )
        else:
            sparc_path = 'samples/sam-{0}/{1}/{2}/{3}/section_{4}/{5}/'\
            'sam-{0}_spec-{1}_lat-{2}_stain-{3}_sec-{4}_mag-{5}{6}'.format(
            self.get_subject_id(), self.get_specimen(), self.get_laterality(),
            self.get_stain(), self.get_section(), self.get_magnification(),
            self.get_filetype()
            )
        return sparc_path
   
   
class ScopeSyntax:
    def __init__(self, file_path):
        self.file_path = file_path

    def scope_format(self):
        if 'sam-' in self.file_path:
            return SparcImage(self.file_path)
        else:
            return UserSyntax(self.file_path)

    def get_zstack(self):
        zstack = os.path.splitext(self.file_path)[0].lower().split('_')[-1]
        if 'z[0-9]' in zstack:
            return zstack
        else:
            return None

    def get_channel(self):
        channel = self.file_path.lower().split('_')[-2][-1]
        if 'ch[0-9]' in channel:
            return channel
        else:
            return 'overlay'
         
         
class UserSyntax(ScopeSyntax):
    def __init__(self, file_path):
        super().__init__(self)
        self.file_path = file_path

    def user_format(self):
        if '5ht/' in self.file_path:
            return Ht(self.file_path)
        if '5ht7' in self.file_path:
            return Ht7(self.file_path)
        if '5ht2a' in self.file_path:
            return Ht2a(self.file_path)
        if '5ht2b' in self.file_path:
            return Ht2b(self.file_path)
        if 'a2a' in self.file_path:
            return a2a(self.file_path)
    
    def get_specimen(self):
        return 'phrenic'

   
class Ht(UserSyntax, SparcImage):
    def __init__(self):
        super().__init__(self)
        self.file_path = file_path

    def get_subject_id(self):
        subject_id = self.file_path.split('/')[-1].split('_')[1]
        if 'section' in subject_id:
            subject_id = file_path.split('/')[-1].split('_')[0].split()[2]
        return subject_id

    def get_laterality(self):
        if self.get_magnification() == '2x':
            return 'whole'
        elif lower(self.file_path.split('/')[-1].split('_')[-3]) in ['il', 'l', 'lft']:
            return 'left'
        elif lower(self.file_path.split('/')[-1].split('_')[-3]) in ['r', 'cl', 'rt']:
            return 'right'
        else:
            return None

    def get_section(self):
        if self.get_magnification() == '2x':
            return self.file_path.split('/')[-1].split('_')[-3][7:]
        elif 'section' in self.file_path.split('/')[-1].split('_')[1]:
            return self.file_path.split('/')[-1].split('_')[1][7:]
        else:
            return self.file_path.split('/')[-1].split('_')[-4][7:]

    def get_magnification(self):
        return self.file_path.split('/')[-1].split('_')[-2]

    def get_stain(self):
        if self.get_channel == 'overlay':
            return self.file_path.split('/')[-5]
        else:
            stains = self.file_path.split('/')[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain

         
class Ht2a(UserSyntax, SparcImage):
    def __init__(self):
        super().__init__(self)
        self.file_path = file_path

    def get_subject_id(self):
        return self.file_path.split('/')[-3]

    def get_laterality(self):
        return self.file_path.split('/')[-2].split()[1]

    def get_section(self):
        return self.file_path.split('/')[-2].split()[3]

    def get_magnification(self):
        return self.file_path.split('/')[-2].split()[4]

    def get_stain(self):
        if self.get_channel == 'overlay':
            return self.file_path.split('/')[-5]
        else:
            stains = self.file_path.split('/')[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain

         
class Ht2b(UserSyntax, SparcImage):
    def __init__(self):
       super().__init__(self)
       self.file_path = file_path

    def get_subject_id(self):
        return self.file_path.split('/')[-3]

    def get_laterality(self):
        return self.file_path.split('/')[-2].split()[1]

    def get_section(self):
        return self.file_path.split('/')[-2].split()[3]

    def get_magnification(self):
        return self.file_path.split('/')[-2].split()[4]

    def get_stain(self):
        if self.get_channel == 'overlay':
            return self.file_path.split('/')[-5]
        else:
            stains = self.file_path.split('/')[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain

         
class Ht7(UserSyntax, SparcImage):
    def __init__(self):
        super().__init__(self)
        self.file_path = file_path

    def get_subject_id(self):
        return self.file_path.split('/')[-3]

    def get_laterality(self):
        return self.file_path.split('/')[-2].split()[1]

    def get_section(self):
        return self.file_path.split('/')[-2].split()[-2]

    def get_magnification(self):
        return self.file_path.split('/')[-2].split()[-1]

    def get_stain(self):
        if self.get_channel == 'overlay':
            return self.file_path.split('/')[-5]
        else:
            stains = self.file_path.split('/')[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain

         
class A2a(UserSyntax, SparcImage):
    def __init__(self):
        super().__init__(self)
        self.file_path = file_path

    def get_subject_id(self):
        return self.file_path.split('/')[-1].split('_')[2]

    def get_laterality(self):
        return self.file_path.split('/')[-1].split('_')[-3]

    def get_section(self):
        return self.file_path.split('/')[-2].split('_')[3][3:]

    def get_magnification(self):
        return self.file_path.split('/')[-2].split('_')[1]

    def get_stain(self):
        if self.get_channel == 'overlay':
            return self.file_path.split('/')[-5]
        else:
            stains = self.file_path.split('/')[-5].split('+')
            stain = stains[int(self.get_channel()) - 1]
            return stain



