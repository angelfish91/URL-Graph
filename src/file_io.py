#!/usr/bin/evn python2.7
# -*- coding: utf-8 -*-

"""
IO模块
数据加载与保存
"""
import os
import sys
import json
import pandas as pd
from logger import logger


def dump_urls(urls, file_path, csv=True):
    """
    :param urls:
    :param file_path:
    :param csv:
    :return:
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
        logger.debug("OLD DATA FIND! REMOVING\t%s" % file_path)
    try:
        if csv:
            df = pd.DataFrame({"url": urls})
            df.to_csv(file_path, index=False)
        else:
            with open(file_path, "w") as fd:
                for line in urls:
                    fd.write(line + "\n")
        logger.debug("URLs has been dump\t%s" % file_path)
    except Exception as e:
        logger.error("%s\tFILE DUMP ERROR %s" % (file_path, str(e)))
        sys.exit(0)


def load_urls(file_path, csv=True):
    """
    :param file_path:
    :param csv:
    :return:
    """
    try:
        if csv:
            df = pd.read_csv(file_path)
            urls = list(df.url)
        else:
            with open(file_path, "r") as fd:
                urls = [_.strip() for _ in fd]
        logger.debug("URLs Count:\t%d" % (len(urls)))
        return urls
    except Exception as e:
        logger.error("%s FILE OPEN ERROR! %s" % (file_path, str(e)))
        sys.exit(0)
        
            
def dump_edge_list(node_list, domain_cate_map, output_path):
    source = [_[0] for _ in node_list]
    target = [_[1] for _ in node_list]
    weight = [_[2] for _ in node_list]
    path = [_[3] for _ in node_list]
    label = [domain_cate_map[_[0]] for _ in node_list]
    ids = [_ for _ in range(len(node_list))]
    types = ["Undirected" for _ in range(len(node_list))]
    df = pd.DataFrame({"Source":source, 
                       "Target":target, 
                       "Weight":weight, 
                       "Id":ids, 
                       "Tpye":types,
                       "Label":label,
                       "path":path})
    df.to_csv(output_path, index=False)

    
def dump_node_list(node_list, domain_cate_map, output_path):
    ids = [_[0] for _ in node_list] + [_[1] for _ in node_list]
    label = [domain_cate_map[_[0]] for _ in node_list] + [domain_cate_map[_[1]] for _ in node_list]
    df = pd.DataFrame({"Id":ids,
                       "Label":label})
    df.to_csv(output_path, index=False)


def dump_check_result(fp, tp, regex_list, file_path):
    """
    :param fp: [list]
    :param tp: [list]
    :param regex_list: [list]
    :param file_path:  [str]
    :return:
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
        logger.debug("OLD DATA FIND! REMOVING\t%s" % file_path)
    try:
        with open(file_path, "w+") as fd:
            for i in range(len(regex_list)):
                fd.write(str(fp[i]) + "\t" + str(tp[i]) +
                         "\t" + regex_list[i] + "\n")
        logger.debug("Check Result has been dump\t%s" % file_path)
    except Exception as e:
        logger.error("%s\tFILE DUMP ERROR %s" % (file_path, str(e)))
        sys.exit(0)


def load_check_result(file_path):
    """
    :param file_path:
    :return:
    """
    try:
        fp_list, tp_list, regex_list = [], [], []
        with open(file_path, "r") as fd:
            for line in fd:
                res = line.strip().split("\t")
                fp_list.append(int(res[0]))
                tp_list.append(int(res[1]))
                regex_list.append(res[2])
        df = pd.DataFrame({"fp": fp_list, "tp": tp_list, "regex": regex_list})
        logger.debug("check Data has been loaded\t%s" % file_path)
    except Exception as e:
        logger.error("%s\t FILE OPEN ERROR! %s" % (file_path, str(e)))
        sys.exit(0)
    return df


# load regular expression data
def load_regex_list(file_path):
    """
    :param file_path:
    :return:
    """
    regex = []
    try:
        with open(file_path, 'r') as fd:
            for line in fd:
                regex.append(line.strip())
        logger.debug("Regex Data has been loaded\t%s" % file_path)
    except Exception as e:
        logger.error("%s\t FILE OPEN ERROR! %s" % (file_path, str(e)))
        sys.exit(0)
    return regex


# load regular expression data
def dump_regex_list(regex_list, file_path):
    """
    :param file_path:
    :param regex_list: [list]
    :return:
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
        logger.debug("OLD DATA FIND! REMOVING\t%s" % file_path)
    try:
        with open(file_path, 'w+') as fd:
            for regex in regex_list:
                fd.write(regex + '\n')
        logger.debug("Regex has been dump\t%s" % file_path)
    except Exception as e:
        logger.error("%s\tFILE DUMP ERROR %s" % (file_path, str(e)))
        sys.exit(0)
        

def load_ac_domain(ac_root_path = "../../data/ac_white", except_category = None):
    if except_category is None:
        except_category = ["色情",
                           "赌博",
                           "非法药物",
                           "钓鱼及恶意网站",
                           "反动及其他非法内容",
                           "成人内容",
                           "钓鱼及恶意网站"]
    urls = list()
    for cate in os.listdir(ac_root_path):
        if cate not in except_category:
            urls.extend(load_urls(os.path.join(ac_root_path, cate), csv = False))
    urls = set(urls)
    logger.info("ac white domain list load:%d" %len(urls))
    return urls

def load_label_data(root_path = "../../data/"):
    porn = []
    gambing = []
    others = []
    die = []
    with open(os.path.join(root_path, "porn_gambing_webside_label.txt"), "r") as f:
        for index, line in enumerate(f):
            if index == 0:
                continue
            domain, label = line.strip().split('\t')[0:2]
            if label == "色情":
                porn.append(domain)
            elif label == "赌博":
                gambing.append(domain)
            elif label != "无法访问":
                others.append(domain)
            else:
                die.append(domain)
    logger.info("\nporn:%d\ngambing:%d\ndie:%d\nothers:%d" \
                %(len(porn), len(gambing), len(others), len(die)))
    return porn, gambing, others, die        
