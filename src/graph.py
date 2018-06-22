# -*- coding: utf-8 -*-
import urlparse
import pandas as pd
import numpy as np
import time
import community
import networkx as nx
from itertools import combinations
from collections import Counter, defaultdict
from joblib import Parallel, delayed


from file_io import load_regex_list, dump_regex_list
from logger import logger


def calc_path_similarity(bpath, cpath):
    assert isinstance(bpath, str) and isinstance(cpath, str)
    if bpath.startswith("/"):
        bpath = bpath[1:]
    if cpath.startswith("/"):
        cpath = cpath[1:]
    if bpath.endswith("/"):
        bpath = bpath[:-1]
    if cpath.endswith("/"):
        cpath = cpath[:-1]
    bpath_token = bpath.split('/')
    cpath_token = cpath.split('/')
    sim_score = 0
    sim_path = ""
    for i in range(min(len(bpath_token), len(cpath_token))):
        if bpath_token[i] != cpath_token[i]:
            break
        sim_score += 2**(i + 1)
        sim_path += "/%s" % bpath_token[i]
    return sim_score, sim_path


def calc_domain_similarity(bdomain, cdomain):
    """
    :param bdomain: list of path of base domain
    :param cdomain: list of path of compare domain
    :return:
    """
    assert isinstance(bdomain, list) and isinstance(cdomain, list)
    comp_res = [(0, "")]
    for bpath in bdomain:
        for cpath in cdomain:
            comp_res.append(calc_path_similarity(bpath, cpath))
    try:
        comp_res = sorted(comp_res, key=lambda x: x[0], reverse=True)
    except Exception as e:
        pass
    return comp_res[0][0], comp_res[0][1]


def build_graph(domain_path_map, thresh=6):
    node_list = []
    node_list_list = []
    domain_list = list(domain_path_map.keys())
    logger.info("Total Domain Count:%d" % len(domain_list))
    logger.info("Total Comp Count:%d" % (len(domain_list)**2 / 2))
    comp_cnt = 0
    for bi, bdomain in enumerate(domain_list):
        for ci, cdomain in enumerate(domain_list[bi + 1:]):
            sim_score, sim_path = calc_domain_similarity(
                domain_path_map[bdomain], domain_path_map[cdomain])
            if sim_score >= thresh:
                node_list.append([bdomain, cdomain, sim_score, sim_path])
            comp_cnt += 1
            if comp_cnt % 500000 == 0:
                # log output current comp count/total comp count, complete ratio
                logger.info("Domain comp step:%d/%d\t%.2f" %
                    (comp_cnt,
                     len(domain_list)**2 / 2,
                     comp_cnt / float(len(domain_list)**2 / 2)))
                logger.debug("%d\t%s" % (len(node_list), node_list[-1]))
                if len(node_list) > 100000:
                    logger.debug("limit exceed %s" %
                                 (str([len(_) for _ in node_list_list])))
                    node_list_list.append(node_list)
                    node_list = []
    if len(node_list_list) > 0:
        for i in node_list_list:
            node_list += i
    return node_list


def _core_build_graph(
        params,
        domain_path_map,
        thresh,
        batch_index,
        verbose=True):
    node_list = []
    for index, (bdomain, cdomain) in enumerate(params):
        sim_score, sim_path = calc_domain_similarity(
            domain_path_map[bdomain], domain_path_map[cdomain])
        if sim_score >= thresh:
            node_list.append([bdomain, cdomain, sim_score, sim_path])
        if verbose and index % 500000 == 0:
            print("batch:%d\t%d/%d" % (batch_index, index, len(params)))
    return node_list


def _batch_split(params, batch_size):
    params_list = []
    params = list(params)
    for i in range(0, len(params), batch_size):
        params_list.append(params[i:i + batch_size])
    return params_list


def build_graph_mp(domain_path_map, thresh=6, batch_size=10000000, n_jobs=8):
    st_time = time.time()
    node_list = []
    domain_list = list(domain_path_map.keys())
    logger.info("Total Domain Count:%d" % len(domain_list))
    logger.info("Total Comp Count:%d" % (len(domain_list)**2 / 2))

    params = combinations(domain_list, r=2)
    params_list = _batch_split(params, batch_size)
    logger.debug("Params ready, time consume:%f" %(time.time()-st_time))

    res_list = Parallel(
        n_jobs=n_jobs)(
        delayed(_core_build_graph)(
            params,
            domain_path_map,
            thresh,
            batch_index) for batch_index,
        params in enumerate(params_list))
    for res in res_list:
        node_list += res
    return node_list


def make_modularity(node_list, domain_cate_map, resolution=1):
    G = nx.Graph()
    for src_node, tar_node, weight, path in node_list:
        G.add_edge(src_node, tar_node, weight = weight, path = path)
    partition = community.community_louvain.best_partition(G, resolution = resolution)
    Id_list, modularity_class_list, Label_list = [],[],[]
    for node, modularity_class in partition.iteritems():
        Id_list.append(node)
        modularity_class_list.append(modularity_class)
        Label_list.append(domain_cate_map[node])
    df = pd.DataFrame({'Id':Id_list, 'modularity_class':modularity_class_list, 
                       'Label':Label_list})
    return df


def modularity_class_analyze(
        input_path,
        min_cluster_thresh=10,
        mis_classify_count_thresh=0):
    """
    :param input_path: Gephi output node.csv
    :param min_cluster_thresh:
    :param mis_classify_count_thresh:
    :return:
    """
    if isinstance(input_path, str):    
        df = pd.read_csv(input_path)
    elif isinstance(input_path, pd.DataFrame):
        df = input_path
    community_dict = defaultdict(list)
    community_count = 0
    community_class = list(set(df.modularity_class))
    logger.info("input community size:%d" % len(community_class))

    for i in community_class:
        df_mc = df.loc[df.modularity_class == i]
        if len(df_mc) < min_cluster_thresh:
            continue
        counter = Counter(df_mc.Label)
        target_label = max(list(counter.iteritems()), key=lambda x:x[1])[0]
        minority_count = sum([_[1] for _ in list(counter.iteritems()) if _[0] != target_label])
        if minority_count <= mis_classify_count_thresh:
            community_dict[target_label].append(list(df_mc.loc[df_mc.Label == target_label].Id))
            community_count += 1
    logger.info("filter community size:%d" % community_count)
    logger.info("partition acc %f" %(sum([len(_) for __ in community_dict.values() for _ in __])/float(len(df))))
    return community_dict


def path_extract(input_path, community_dict):
    df = pd.read_csv(input_path)
    label_path_map = defaultdict(list)
    for label, comm_list in community_dict.iteritems():
        for comm in comm_list:
            df_sp = df.loc[[_ in set(comm) for _ in df.Source]]
            df_sp = df_sp.loc[[_ in set(comm) for _ in df_sp.Target]]

            df_xp = df.loc[[_ in set(comm) for _ in df.Target]]
            df_xp = df_xp.loc[[_ in set(comm) for _ in df_xp.Source]]

            df_ap = pd.concat([df_sp, df_xp])
            label_path_map[label].extend(list(df_ap.path))
    res =dict()
    for label, path in label_path_map.iteritems():
        res[label] = [ _ for _ in set(path) if _.count('/') >=2]
        logger.info("label:%s\tpath extract:%d" %(label, len(res[label])))
    return res
