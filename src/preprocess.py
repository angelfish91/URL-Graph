# -*- coding: utf-8 -*-
import os
import urlparse
from collections import defaultdict

from logger import logger
from file_io import load_urls, dump_urls


def make_domain_path_split(urls, path_level_thresh = 1):
    """
    make domain map to path for preparation of build graph
    :param urls:
    :return:
    """
    domain_path_map = defaultdict(list)
    for url in urls:
        try:
            url_obj = urlparse.urlparse(url)
            if len(url_obj.path)>1 and url_obj.path.count('/') >= path_level_thresh:
                domain_path_map[url_obj.hostname].append(url_obj.path)
        except Exception as e:
            pass
    return domain_path_map



def ac_map_local(domain_path_map, category = None, ac_root_path = "../../data/ac_white"):
    """
    :param domain_path_map:
    :param category:
    :param ac_root_path:
    :return:
    """
    assert isinstance(domain_path_map, dict)
    assert isinstance(category, list) or category is None
    domain_set = set(domain_path_map.keys())
    if category is None:
        category = ["色情", "赌博"]
    domain_list_in_cate = []
    cate_digit_map = dict(zip(category, range(len(category))))
    domain_cate_map = dict()
    for cate in category:
        ac_cate_domain_list = load_urls(os.path.join(ac_root_path, cate), csv = False)
        domain_list_in_cate.append(list(set(ac_cate_domain_list) & domain_set))
        logger.info("%s\tcount:%d" %(cate, len(domain_list_in_cate[-1])))
        for domain in domain_list_in_cate[-1]:
            domain_cate_map[domain] = cate_digit_map[cate]
    cate_domain_map = dict(zip(category, domain_list_in_cate))
    domain_list_in_cate = [_ for domain_list in domain_list_in_cate for _ in domain_list]
    domain_path_map = dict(zip(domain_list_in_cate, [domain_path_map[_] for _ in domain_list_in_cate]))
    return cate_domain_map, domain_cate_map, domain_path_map
