#-*- coding: utf-8 -*-
import pymongo
import MySQLdb
import re
import json
import hashlib
import logging
import tqdm
from pyquery import PyQuery as pyq
from elasticsearch import Elasticsearch, helpers
from settings import conf
import datetime
_MONGO_DB = None  # mongo mysql cursor
_ES_INSTANCE = None  # _MYSQL_CURSOR = None


def mongo_db(): 
    global _MONGO_DB
    if _MONGO_DB is None:
        mongo_host = conf['mongo']['host']
        mongo_port = conf['mongo']['port']
        client = pymongo.MongoClient(mongo_host, mongo_port)
        _MONGO_DB = client.edxapp
    return _MONGO_DB


def mysql_connection():
    mysql_host = conf['mysql']['host']
    mysql_user = conf['mysql']['user']
    mysql_password = conf['mysql']['password']
    conn_mysql = MySQLdb.connect(*[mysql_host, mysql_user, mysql_password, 'edxapp'], charset='utf8', use_unicode=True)
    return conn_mysql


def es_instance():
    global _ES_INSTANCE

    if _ES_INSTANCE is None:
        _ES_INSTANCE = Elasticsearch(conf["es"]["host"], sniffer_timeout=60, timeout=60)
    return _ES_INSTANCE


def clear_es_data(_index, doc_type):
    es = es_instance()
    es.delete_all(index=_index, doc_type=doc_type)


def put_mapping(_index, doc_type, mapping_file = 'es_setting.json'):
    f = open(mapping_file)
    mapping = json.loads(f.read())
    es = es_instance()
    es.put_mapping(index=_index, doc_type = doc_type, mapping = mapping)


def md5(content):
    return hashlib.sha1(content).hexdigest()


class CourseDescriptor(object):
    def __init__(self, course_id):
        # base info
        self.course_id = course_id
        self.course_meta_id = -1
        self.id_ = ""
        self.org = ""
        self.course = ""
        self.run = ""
        self.course_hash = ""
        self.owner = ""
        self.serialized = ""
        self.cid = []
        self.start = ""
        self.end = ""
        self.status = 0
        # searchable info
        self.course_name = ""
        self.about = ""
        self.prerequisites = ""
        self.title_type = 'title'
        self.about_type = 'about'
        self.staff_type = 'staff'
        self.subtitle_type = 'subtitle'
        self.prerequisites_type = 'prerequisites'
        # chunks
        self.title_chunks = []
        self.about_chunks = []
        self.structure_chunks = []
        #self.problem_chunks = []
        #self.html_chunks = []
        self.staff_chunks = []
        self.prerequisites_chunks = []
        self.subtitle_chunks = []
        self.fragment_chunks = []
        # structure info
        self.children = []
        self.chapters = []
        self.staff = []
        self.fragment = []
        self.db = mongo_db()
        self.es = es_instance()
        self.mysql_conn = mysql_connection()
        self.cursor = self.mysql_conn.cursor()
        self._init_course_data()
        self.location = [self.course_id]
        
    def _init_course_data(self):
        self.cursor.execute("""\
                select course_num, 
                org, 
                run, 
                name, 
                about, 
                owner, 
                serialized, 
                id, 
                start,
                end, 
                prerequisites,
                subtitle,
                status,
                course_type
                from course_meta_course where course_id = '{}';
                """.format(self.course_id))
        course_info = self.cursor.fetchall()[0]
        self.course = course_info[0]
        self.org = course_info[1]
        self.run = course_info[2]
        self.course_name = course_info[3]
        self.about = course_info[4]
        self.owner = course_info[5]
        self.serialized = course_info[6]
        self.course_meta_id = int(course_info[7])
        self.cursor.execute("select coursecategory_id from course_meta_course_category where course_id = '{}'".format(self.course_meta_id))
        self.cid = []
        if course_info[8] == None:
            self.start = '2030-01-01 00:00:00'
        else:
            self.start = datetime.datetime.strftime(course_info[8], "%Y-%m-%d %H:%M:%S")
        
        if course_info[9] == None:
            self.end = '2030-01-01 00:00:00'
        else:
            self.end = datetime.datetime.strftime(course_info[9], "%Y-%m-%d %H:%M:%S")
        category_list = self.cursor.fetchall()
        for category in category_list:
            self.cid.append(category[0])
        _id = {
                'category': 'meta',
                'tag': 'i4x',
                'course': course_info[0],
                'org': course_info[1],
                'name': course_info[2],
                'revision': 'null'
                }
        self.id_ = json.dumps(_id)
        # 预备知识
        self.prerequisites = course_info[10]
        self.subtitle = course_info[11]
        self.course_hash = md5(self.course_id)
        self.status = int(course_info[12])
        self.course_type = int(course_info[13])
        # 碎片化
        self._init_fragment()
        # 教师
        self._init_staff()
    
    def _init_staff(self):
        self.cursor.execute("""\
                select re.id, 
                st.name, 
                st.company,
                st.department,
                st.position
                from course_meta_staff as st, 
                course_meta_coursestaffrelationship as re, 
                course_meta_course as co 
                where st.id = re.staff_id 
                and co.id = re.course_id 
                and co.id = '{}'\
                        """.format(self.course_meta_id))
        staff_list = self.cursor.fetchall()
        for staff in staff_list:
            self.staff.append({
                "relation_id": staff[0],
                "searchable_text" : {
                    "name": staff[1],
                    'company': staff[2],
                    'department': staff[3],
                    'position': staff[4],
                    'searchable_text': ' '.join(staff[1:]).strip()
                }
            })
    
    def _init_fragment(self):
        self.cursor.execute("""\
                select fr.title, 
                fr.description,
                fr.id,
                fr.is_active
                from course_meta_fragmentknowledge as fr 
                where course_id = '{}'\
                        """.format(self.course_id))
        fragment_list = self.cursor.fetchall()
        for fragment in fragment_list:
            self.fragment.append({
                "fragment_id": fragment[2],
                "status": int(fragment[3]) - 1,
                "searchable_text" : {
                    "title" : fragment[0],
                    "desc" : fragment[1]
                    }
                })

    def _traverse_children(self):
        chapter_list = []
        sequential_list = []
        vertical_list = []
        for item in self.children:
            if item.category == "chapter":
                item.location += [item.item_id]
                chapter_list.append(item)
            elif item.category == "sequential":
                sequential_list.append(item)
            elif item.category == "vertical":
                vertical_list.append(item)

        def _init_location(parent_list, child_list):        
            for parent in parent_list:
                for child in child_list:
                    for parent_child in parent.children:
                        if child.item_id == parent_child[1]:
                            child.location = parent.location + [child.item_id]
                            break

        _init_location(chapter_list, sequential_list)
        _init_location(sequential_list, vertical_list)

    def build_chunks(self):
        self.build_title_chunks()
        self.build_about_chunks()
        self.build_staff_chunks()
        self.build_subtitle_chunks()
        self.build_fragment_chunks()
        for child in self.children:
            self.build_child_chunks(child)

    def build_title_chunks(self):
        source = self.get_base()
        source['item_type'] = self.title_type
        source['searchable_text'] = self.course_name
        if self.course_name != "":
            self.title_chunks = [self.es.index_op(source, id=self.course_hash)]
    
    def build_subtitle_chunks(self):
        source = self.get_base()
        source['item_type'] = self.subtitle_type
        source['searchable_text'] = self.subtitle
        if self.subtitle != "":
            self.subtitle_chunks = [self.es.index_op(source, id=self.course_hash)]
   
    def build_about_chunks(self):
        source = self.get_base()
        source['item_type'] = self.about_type
        source['searchable_text'] = self.get_about_searchable_text()
        if self.get_about_searchable_text() != "":
            self.about_chunks = [self.es.index_op(source, id=self.course_hash)]
    
    def build_prerequisites_chunks(self):
        source = self.get_base()
        source['item_type'] = self.prerequisites_type
        source['searchable_text'] = self.prerequisites
        self.prerequisites_chunks = [self.es.index_op(source, id=self.course_hash)]

    def build_staff_chunks(self):
        for _staff in self.staff:
            relation_id = _staff['relation_id']
            source = self.get_base()
            info = _staff['searchable_text']
            source['item_type'] = self.staff_type + '-info'
            source['searchable_text'] = info.pop('searchable_text')
            if source['searchable_text'] == "":
                continue
            for (k, v) in info.items():
                source[k] = v.strip()
            self.staff_chunks.append(self.es.index_op(source, id=relation_id))

    
    def build_fragment_chunks(self):
        for fr in self.fragment:
            fragment_id = fr['fragment_id']
            source = self.get_base()
            source['knowledge_id'] = str(fragment_id)
            source['status'] = fr['status']
            is_empty = True
            for (name, value) in fr['searchable_text'].items():
                if value != "":
                    is_empty = False
                source['frag_' + name] = value
            if not is_empty:
                self.fragment_chunks.append(self.es.index_op(source, id=fragment_id))
    
    def build_child_chunks(self, child):
        source = self.get_base()
        source['item_type'] = child.item_type
        source['searchable_text'] = child.searchable_text
        if child.searchable_text == "":
            return
        if child.item_type == 'structure':
            source['location'] = child.location
            source['order_num'] = child.order_num
            self.structure_chunks.append(self.es.index_op(source, id=child.item_hash()))

    def get_searchable_text(self, data):
        if data:
            p = re.compile(r'<.*?>')
            tag_removed = p.sub('', data)
            tag_removed = tag_removed.replace('<', '&lt;').replace('>', '&gt;')
            return tag_removed
        else:
            return ""

    def get_about_searchable_text(self):
        return self.get_searchable_text(self.about)
    
    def get_base(self):
        source = {
                'course_id': self.course_id,
                'org': self.org,
                'owner': self.owner,
                'serialized': self.serialized,
                'cid': self.cid,
                'start': self.start,
                'end': self.end,
                'course_meta_id': self.course_meta_id,
                'status': self.status,
                'location': self.location
                }
        return source
    def close_mysql(self):
        if self.cursor:
            self.cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()


class OldCourseDescriptor(CourseDescriptor):
    def __init__(self, course_id):
        CourseDescriptor.__init__(self, course_id)
        self._init_children()
        self._traverse_children()
    
    def _init_children(self):
        course = self.db.modulestore.find_one({'_id.org': self.org, '_id.course': self.course, '_id.category': 'course'})
        if course:
            course_info = course.get('definition', {}).get('children', [])
            order_num = [0, 0]
            for chapter in course_info:
                chapter_id = chapter.split('/')[-1]
                order_num[0] += 1
                chapter_item = self.db.modulestore.find_one({'_id.org': self.org, '_id.course': self.course, '_id.category': 'chapter', '_id.name': chapter_id})
                if chapter_item != None:
                    self.children.append(OldStructureItem(self, chapter_item, order_num))
                    for sequential in chapter_item.get('definition', {}).get('children', []):
                        seq_id = sequential.split('/')[-1]
                        seq_item = self.db.modulestore.find_one({'_id.org': self.org, '_id.course': self.course, '_id.category': 'sequential', '_id.name': seq_id})
                        order_num[1] += 1
                        if seq_item != None:
                            self.children.append(OldStructureItem(self, seq_item, order_num))
                order_num[1] = 0


class NewCourseDescriptor(CourseDescriptor):
    def __init__(self, course_id):
        CourseDescriptor.__init__(self, course_id)
        self._init_children()
        self._traverse_children()

    def _init_children(self):
        course = self.db.modulestore.active_versions.find_one({'org': self.org, 'course': self.course, 'run': self.run},{'versions': 1})
        if course:
            version = course['versions']['published-branch']
            course_struct = self.db.modulestore.structures.find_one({'_id': version})
            order_num = [0,0]
            blocks = course_struct.get('blocks', [])
            for block in blocks:
                block_type = block['block_type']
                block_id = block['block_id']
                if block_type == 'course':
                    chapters = block.get('fields', {}).get('children', [])
                    for chapter in chapters:
                        chapter_id = chapter[1]
                        for chapter_item in blocks:
                            if chapter_item['block_id'] == chapter_id:
                                order_num[0] += 1
                                self.children.append(NewStructureItem(self, chapter_item, order_num))
                                for sequential in chapter_item.get('fields', {}).get('children', []):
                                    seq_id = sequential[1]
                                    for seq_item in blocks:
                                        if seq_item['block_id'] == seq_id:
                                            order_num[1] += 1
                                            self.children.append(NewStructureItem(self, seq_item, order_num))
                                order_num[1] = 0

    def build_index(self):
        # title
        pass


class Module(object):
    def __init__(self, course):
        # add course info for module
        self.course_id = course.course_id
        self.org = course.org
        self.course = course.course
        self.run = course.run
        self.category = ""
        self.item_id = ""
        # _id = hash(course_id)
        # self.id_ = course.id_
        self.course_hash = course.course_hash
        # course tree
        self.location = [course.course_id]
        self.db = course.db

    def item_hash(self):
        _id = {
                'tag': 'i4x',
                'org': self.org,
                'course': self.course,
                'name': self.item_id,
                'category': self.category,
                'revision': 'null'
                }
        return hashlib.sha1(json.dumps(_id)).hexdigest()


class StructureItem(Module):
    def __init__(self, course, order_num):
        Module.__init__(self, course)
        self.index = 'structure-index'
        self.item_type = 'structure'
        order_str = []
        for item in order_num:
            if item < 10:
                order_str.append("0" + str(item))
            else:
                order_str.append(str(item))
        self.order_num = '.'.join(order_str)

    def get_searchable_text(self, data):
        pass


class NewStructureItem(StructureItem):
    def __init__(self, course, data, order_num):
        StructureItem.__init__(self, course, order_num)
        self.item_id = data.get('block_id', '')
        self.searchable_text = self.get_searchable_text(data)
        self.category = data.get('block_type', '')
        self.children = data.get('fields', {}).get('children', [])

    def get_searchable_text(self, data):
        return data.get('fields', {}).get('display_name', '')


class OldStructureItem(StructureItem):
    def __init__(self, course, data, order_num):
        StructureItem.__init__(self, course, order_num)
        self.item_id = data['_id']['name']
        self.category = data['_id']['category']
        self.searchable_text = self.get_searchable_text(data)
        children = data.get('definition', {}).get('children', [])
        self.children = map(lambda x: x.split('/')[-2:], children)

    def get_searchable_text(self, data):
        return data.get('metadata', {}).get('display_name', '')


class ProblemItem(Module):
    def __init__(self, course):
        Module.__init__(self, course)
        self.index = 'problem-index'
        self.item_type = 'problem'
        self.category = 'problem'

    def get_searchable_text(self, data):
        pass

    def remove_tag(self, data):
        if data:
            if type(data) == dict:
                data = data.get('data', '')
            p = re.compile(r'<.*?>')
            tag_removed = p.sub('', data)
            tag_removed = tag_removed.replace('<', '&lt;').replace('>', '&gt;').replace('Explanation', ' ').replace('\n', ' ')
            return ' '.join(tag_removed.split())
        else:
            return ''


class NewProblemItem(ProblemItem):
    def __init__(self, course, data):
        ProblemItem.__init__(self, course)
        self.item_id = data.get('block_id', '')
        self.searchable_text = self.get_searchable_text(data)

    def get_searchable_text(self, data):
        definition_id = data.get('definition', '')
        definition = self.db.modulestore.definitions.find_one({'_id': definition_id}) 
        if definition:
            content = definition.get('fields', {}).get('data', None)
            return self.remove_tag(content)
        else:
            return ''


class OldProblemItem(ProblemItem):
    def __init__(self, course, data):
        ProblemItem.__init__(self, course)
        self.item_id = data['_id']['name']
        self.searchable_text = self.get_searchable_text(data)

    def get_searchable_text(self, data):
        definition = data.get('definition', {}).get('data', None) 
        return self.remove_tag(definition)


class HtmlItem(Module):
    def __init__(self, course):
        Module.__init__(self, course)
        self.index = 'html-index'
        self.item_type = 'html'
        self.category = 'html'

    def get_searchable_text(self, data):
        pass

    def format_html(self, html):
        if html == None and html == '':
            return ''
        try:
            doc = pyq(html)
            text = doc.text()
            result = ' '.join(text.replace('\n', ' ').split())
        except Exception, e:
            result = html
        finally:
            return result


class NewHtmlItem(HtmlItem):
    def __init__(self, course, data):
        HtmlItem.__init__(self, course)
        self.item_id = data.get('block_id', '')
        self.searchable_text = self.get_searchable_text(data)

    def get_searchable_text(self, data):
        definition_id = data.get('definition', '')
        definition = self.db.modulestore.definitions.find_one({'_id': definition_id})
        if definition:
            content = definition.get('fields', {}).get('data', '')
            if content == '':
                return ''
            return self.format_html(content)
        else:
            return ''


class OldHtmlItem(HtmlItem):
    def __init__(self, course, data):
        HtmlItem.__init__(self, course)
        self.item_id = data['_id']['name']
        self.searchable_text = self.get_searchable_text(data)

    def get_searchable_text(self, data):
        definition = data.get('definition', {}).get('data', '')
        if definition != '':
            return self.format_html(definition)
        else:
            return ''


