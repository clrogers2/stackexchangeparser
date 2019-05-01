import re
import os
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import subprocess
try:
    from prodigy import log
except ImportError:
    from .utils import log
from .utils import find_program, capture_7zip_stdout


class StackExchangeParser(object):
    
    class _TagStripper(HTMLParser):
        """
        HTML Parser that receives a string with HTML tags, strips out tags. get_data() will return a string devoid of HTML tags.
        
        """
        def __init__(self, convert_charrefs=True):
            super().__init__()
            self.reset()
            self.strict = False
            self.convert_charrefs = convert_charrefs
            self.fed = []

        def handle_data(self, d):
            self.fed.append(d)

        def get_data(self):
            return ''.join(self.fed)

        def error(self, message):
            pass
    
    def __init__(self, file, community=None, content_type='post_body', newlines=True, onlytags=None):
        """
        A Prodigy compliant corpus loader that reads a StackExchange xml file (or list of community urls) and yields a
        stream of text in dictionary format.
        
        :param file: None or string path name to xml file. If None, read files from Archive.org using communities param.
        :param community: string, name of StackExchange community
        :param content_type: string, select the type of text to return: [post_title, post_body, post_both, comments]
        :param newlines: Boolean, If True, keep newlines in text, if False, replace newlines with space.
        :param onlytags: Only return posts which contain one or more of the provided tags
        """
        self.iter = iter(self)
        # Check that the path actually exists and is a recognizable XML file
        self.URL = 'https://archive.org/download/stackexchange/'
        self.communities, self.latest_data_date = self._get_community_names()

        # Regex to find newlines
        self.newlines = newlines
        self.newline = re.compile(r'\n+')

        # Acceptable types of StackExchange text content
        self._TYPES = ['post_title', 'post_body', 'post_both', 'comments']
        assert (content_type.lower() in self._TYPES), " Acceptable content_types include {}".format(self._TYPES)
        self.content_type = content_type

        if not file:
            self.community = self._verify_community_names(community)
            log('STREAM: Attempting to download {}'.format(self.community))
            download_file = self._download_community(self.community)
            log('STREAM: {} downloaded. Attempting to decompress Posts.xml file'.format(download_file))
            file = self._rename_and_extract_7zip(download_file)

        if '.7z' in file:
            file = self._rename_and_extract_7zip(file)

        se_file = Path(file).absolute()
        # If user provides an xml file, but doesn't supply the community name, try to determine the community from
        # either the path or the name of the file. If no community is identified, set community to 'unknown'
        if not community:
            test_com = se_file.parent.parts[-1]
            test_name = se_file.name.split('_')
            if '.com' in test_com:
                community = test_com
            elif len(test_name) > 1:
                if 'stackoverflow' not in test_name[0]:
                    community = test_name[0] + '.stackexchange.com'
                else:
                    community = test_name[0] + '.com'
            else:
                community = 'unknown'
        self.community = self._verify_community_names(community)

        # ensure the file exists and is now in xml format
        assert (se_file.exists()), "Cannot find file. Please check the path name and try again"
        assert (se_file.suffix == '.xml'), "File does not end in '.xml'. Please check the path name and try again"
        log('STREAM: {} file found'.format(se_file.as_posix()))
        self.file = se_file

        # Lazily load the xml file, puts a blocking lock on the file
        self.tree = ET.iterparse(self.file.as_posix(), events=['end'])
        
        # To identify even more specific results a user can supply a StackExchange tag or tags.
        # Only Posts with one or more tags will be returned
        if type(onlytags) == str:
            onlytags = [onlytags]
        self.onlytags = onlytags
        
        # Keep a count of total and parsed rows
        self.total = 0
        self.parsed = 0
        
        # Keep track of Question tags for use by Answer Posts
        # Also keep a count of the number of expected answers and the number of seen answers
        self.parent_post_tags = {}
    
    def _get_community_names(self):
        URL = self.URL
        page = requests.get(URL)
        page_html = BeautifulSoup(page.text, 'lxml')
        div = page_html.find('div', class_='download-directory-listing')
        coms, dates = [], set()
        for row in div.select('table.directory-listing-table tr'):
            contents = row.contents
            com = str(contents[1].contents[0].contents[0])
            if '7z' in com:
                coms.append(com.replace('.7z', ''))
                d = contents[3].contents[0][:11]
                dates.add(d[3:].replace('-', ''))

        return coms, dates.pop()

    def _verify_community_names(self, com):
        if com == 'unknown':
            return com
        else:
            assert isinstance(com, str), "Community name must be a string"
            if com not in self.communities:
                log("STREAM: {com} not found in online archive at {url}".format(com=com, url=self.URL))
            assert (com in self.communities), """StackExchange community--{com}--not found in online archive at {url}""".format(com=com, url=self.URL)
            return com

    def _rename_and_extract_7zip(self, file):
        program = find_program()
        if program is None:
            raise EnvironmentError("7-Zip not found in OS environment. Archive cannot be extracted")

        # Path might have a partial or full path, convert to Path object, check if it exists, then pass just the name
        # to
        se_file_name = Path(file).absolute()
        assert (se_file_name.exists()), "Cannot find {} file. Please check the path name and try again"\
            .format(se_file_name.as_posix())
        file_name = se_file_name.name
        file_path = se_file_name.as_posix()

        posts = 'Posts' in file_name
        comments = 'Comments' in file_name
        tags = 'Tags' in file_name
        com_name = file_name.split('.')[0]
        if posts or (not posts and not comments and not tags):

            input_name = 'Posts.xml'
            self.type = input_name
            output_name = '{}_Posts.xml'.format(com_name)
        elif comments:
            input_name = 'Comments.xml'
            self.type = input_name
            output_name = '{}_Comments.xml'.format(com_name)

        elif tags:
            input_name = 'Tags.xml'
            self.type = input_name
            output_name = '{}_Tags.xml'.format(com_name)

        else:
            self.type = None
            return None
        parent = se_file_name.parent

        archive_details = capture_7zip_stdout('{prog}7z l -ba -slt "{file}"'.format(prog=program, file=file_path))
        if archive_details.get(input_name, None):
            subprocess.call('{prog}7z rn -ba "{file}" "{fin}" "{fout}"'.format(prog=program, file=file_path, fin=input_name,
                                                                           fout=output_name))
        subprocess.call('{prog}7z e -ba "{file}" "{fout}" -aoa'.format(prog=program, file=file_path, loc=parent,
                                                                   fout=output_name))
        return se_file_name.parent.joinpath(output_name).as_posix()

    def _generate_file_markers(self, file_obj, mem_size=50, mem_unit='MB', order='default'):
        ORDERS = ['default', 'beginning', 'ending', 'shuffle']
        UNITS = {'GB': 4**1024, 'MB': 3**1024, 'KB': 2**1024, 'B': 1}
        if order.lower() not in ORDERS:
            order = 'default'
        if mem_unit not in UNITS.keys():
            mem_unit = 'MB'

        _mem_size = mem_size * UNITS[mem_unit]
        pass

    def _download_community(self, community):
        url = self.URL + community + '.7z'
        local_filename = url.split('/')[-1]

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

        return local_filename

    def _parse_tags(self, tags):
        """
        Parse the Tags attribute of a row in a StackExchange Posts xml file
        
        :param tags: string, tags formatted between <>
        :returns: List of tags or None
        """
        
        if tags == '':
            return None
        else:
            t = re.compile('<(.+?)>')
            m = t.findall(tags)
            return m

    def __next__(self):

        return self.iter.__next__()

    def __iter__(self):
        
        # Iterate through the file and yield the text
        for _, child in self.tree:
            
            self.total += 1
            log("STREAM: Fetching {} child element".format(self.total))
            # Start of file, check that the file matches the expected content_type
            if child.tag != 'row':
                if self.content_type in self._TYPES[:3]:
                    assert(child.tag == 'posts'), "Input file is not a StackExchange Posts.xml file. \
                    Please check the path name and try again"

                else:
                    assert(child.tag == 'comments'), "Input file is not a StackExchange Comments.xml file. \
                    Please check the path name and try again"

            else:   
                # Assemble the prodigy stream compliant dictionary object
                info = {"meta": {"source": "StackExchange", "Community": self.community, "type": self.type}}

                atb = child.attrib
                # Fetch the necessary information
                if self.content_type in self._TYPES[:3]:
                    Id = int(atb.get('Id', None))
                    title = atb.get('Title', None)
                    body = atb.get('Body', None)
                    tags = atb.get('Tags', None)
                    if tags is not None:
                        tags = self._parse_tags(tags)
                    posttype = atb.get('PostTypeId', None)
                    create = atb.get('CreationDate', None)
                    answers = int(atb.get('AnswerCount', 0))
                    
                    # Preserve Tag information from Questions for reference by Answers
                    if posttype == 1:
                        if answers > 0:
                            self.parent_post_tags[Id] = {'tags': tags, 'title': title, 'answers': answers, 'seen': 0}
                        parentid = None

                    # If this post is an answer, lookup the tags on the parent question        
                    elif posttype == 2:
                        parentid = int(atb.get("ParentId", None))
                        parent = self.parent_post_tags.get(parentid, None)
                        
                        if parent:
                            tags = parent.get('tags', None)
                            title = self.parent_post_tags.get(title, None)
                        else:
                            tags = None
                            title = None
                        
                        # If a parent answer value is found, update the seen answer count and
                        # check if all answers have been seen
                        if parent:
                                                    
                            # TODO: Test this code to verify it works as intended
                            # Update the seen answer count
                            if parent['seen'] < parent['answers']:
                                parent['seen'] += 1

                            # Now check if we've seen all the answers,
                            # If so, then delete the Parent Id entry to free up memory
                            if parent['seen'] == parent['answers']:
                                del self.parent_post_tags[parentid]    
                            
                    
                    # If the user only wants text from Posts with specific stackExchange tags,
                    # only return content that matches. Naively iterates through the stream of Posts.
                    # It does not know if the tag actually exists.
                    # If the user supplies tags, and no tags match, skip this post
                    if self.onlytags and not tags:
                        continue
                    elif self.onlytags and not any([tag for tag in tags if tag in self.onlytags]):
                        continue
                    else:   
                        
                        if self.content_type == 'post_both':
                            if title and body:
                                text = title + '\n' + body
                            elif body and not title:
                                text = body
                            elif title and not body:
                                text = title
                            else:
                                text = None

                        elif self.content_type == 'post_title':
                            text = title

                        elif self.content_type == 'post_body':
                            text = body

                elif self.content_type == 'comments':
                    if self.onlytags:
                        print("Tags are currently only available for Posts. Skipping search for {}".format(self.onlytags))
                    text = atb.get('Text', None)

                else:
                    text = None

                # Check to see if valid text was found, if not, skip to the next xml child element
                if text is None:
                    continue

                else:
                    # unescape HTML encoding and remove html tags
                    # Preserve the original HTML
                    info['html'] = text

                    # TODO: Sometimes causes problems in the HTML stripper, disable for now, investigate later
                    #text = html.unescape(text)
                    
                    # HTML Stripper
                    stripper = self._TagStripper()
                    stripper.feed(text)
                    text = stripper.get_data()
                    
                    # Remove extra newlines or all newlines
                    if self.newlines:
                        cleantext = self.newline.sub(r'\n', text)
                    else:
                        cleantext = self.newline.sub(r' ', text)
                    
                    # Append the text and additional metadata to the stream dictionary
                    info['text'] = cleantext
                    info['meta']['ID'] = Id
                    if posttype == 2:
                        info['meta']['ParentTitle'] = title
                    else:
                        info['meta']['title'] = title
                    info['meta']['CreationDate'] = create
                    info['meta']['Tags'] = tags
                    info['meta']['FavoriteCount'] = int(atb.get('FavoriteCount', 0))
                    info['meta']['PostScore'] = int(atb.get('Score', 0))
                    info['meta']['CommentCount'] = int(atb.get('CommentCount', 0))
                    info['meta']['Views'] = int(atb.get('ViewCount', 0))
                    aa = atb.get('AcceptedAnswerId', None)
                    if aa is not None:
                        aa = int(aa)
                    info['meta']['AcceptedAnswer'] = int(aa) if aa is not None else aa
                    info['meta']['LastEdit'] = atb.get('LastEditDate', None)
                    info['meta']['LastActivity'] = atb.get('LastActivityDate', None)

                    # yield the dictionary
                    self.parsed += 1
                    log("STREAM: {p} of {t} XML child element parsed".format(p=self.parsed, t=self.total), info)
                    yield info

            # clear the child from memory before moving to the next child element
            child.clear()

  