# -*- coding: utf-8 -*-
import os
import re
import time
import urlparse
import pandas as pd
from collections import defaultdict
from joblib import Parallel, delayed


from file_io import dump_check_result, load_check_result, dump_regex_list, load_regex_list
from logger import logger


def _calc_regex_score(regex):
    sig = regex.count("/")
    return sig


# convert string to regular expression
def _regularize_string(string):
    """
    transfer_mean = "$()*+.[]?\^{},|"
    :param string:
    :return:
    """
    string = string.replace("$", "\$")
    string = string.replace("(", "\(")
    string = string.replace(")", "\)")
    string = string.replace("*", "\*")
    string = string.replace("+", "\+")
    string = string.replace(".", "\.")
    string = string.replace("[", "\[")
    string = string.replace("]", "\]")
    string = string.replace("?", "\?")
    string = string.replace("^", "\^")
    string = string.replace("{", "\{")
    string = string.replace("}", "\}")
    string = string.replace(",", "\,")
    string = string.replace("|", "\|")
    return string


def _change_digit_to_regex(string):
    tokens = string.split("/")
    if len(tokens) < 2:
        return string
    for index, token in enumerate(tokens):
        subtokens = token.split("-")
        for subindex, subtoken in enumerate(subtokens):
            if subtoken.isdigit():
                subtokens[subindex] = "\d{%d}"%len(subtoken)
        tokens[index] = token = "-".join(subtokens)
    return "/".join(tokens)


def convert_to_regex(path_list, change_digit = False):
    regex_list = [_regularize_string(_) for _ in path_list]
    if change_digit:
        regex_list = list(set([_change_digit_to_regex(_) for _ in regex_list]))
    return regex_list

def url_regex_path_match(regex, url):
    url_path = urlparse.urlparse(url).path
    pattern = re.compile(regex)
    if pattern.match(url_path):
        return True
    return False


def illegal_domain_evaluate(urls, regex_list, white_domain, trainset_domain, result_file_path):
    st_time = time.time()
    fp_list = []
    tp_list = []
    independent_detection_list = []
    fp_domain_map = defaultdict(set)
    detect_domain_map = defaultdict(set)
    for regex in regex_list:
        detection_cnt = 0
        tp_cnt = 0
        for url in urls:
            if url_regex_path_match(regex, url):
                detection_cnt += 1
                if urlparse.urlparse(url).hostname in trainset_domain:
                    tp_cnt += 1
                elif urlparse.urlparse(url).hostname in white_domain:
                    fp_domain_map[regex].add(urlparse.urlparse(url).hostname)
                else:
                    detect_domain_map[regex].add(urlparse.urlparse(url).hostname)
        independent_detection_cnt = detection_cnt - tp_cnt
        print "%40s\tindependent detection %d\ttp %d\tfp %d" % (regex, independent_detection_cnt, tp_cnt, len(fp_domain_map[regex]))
        fp_list.append(len(fp_domain_map[regex]))
        tp_list.append(tp_cnt)
        independent_detection_list.append(independent_detection_cnt)
    df = pd.DataFrame({"path": regex_list, "independent detection": independent_detection_list, "tp": tp_list, "fp": fp_list})
    dump_check_result(fp_list, independent_detection_list, regex_list, result_file_path)
    logger.info("eveluate time consume:%f"%(time.time()-st_time))
    return df, fp_domain_map, detect_domain_map


def regex_publish(input_path,
                  output_path,
                  publish_score,
                  publish_fp_thresh=1,
                  publish_tp_thresh=1):
    """
    :param input_path:
    :param output_path:
    :param publish_score:
    :param publish_fp_thresh:
    :param publish_tp_thresh:
    :return:
    """
    df = load_check_result(input_path)
    df["score"] = [_calc_regex_score(_) for _ in df.regex]
    # dump regular expression with 0 fp
    df = df.loc[df.score >= publish_score]
    df = df.loc[df.fp <= publish_fp_thresh]
    df = df.loc[df.tp >= publish_tp_thresh]

    regex_list_publish = list(set(df.regex))
    df = df.drop_duplicates("regex")
    df = df.set_index("regex")
    df = df.loc[regex_list_publish]
    logger.info("regular expression publish\t%d" % len(regex_list_publish))
    # dump regular expressions
    dump_regex_list(regex_list_publish, output_path)
    return df


# support function for malicious_url_predict
def _core_predict(regex_list, test_urls, batch_index, n_jobs, verbose=True):
    """
    测试模块
    返回regex与测试中的url的映射
    split regular expression into batches for multi-process check
    :param regex_list: regular expressions to check [list]
    :param test_urls: white list url [list]
    :param batch_index: batch index
    :param n_jobs: n jobs for multi-process
    :return:
    """
    # decides which batch of regular expression use to check
    batch_size = int(len(regex_list) / n_jobs)
    start_index = batch_index * batch_size
    end_index = (batch_index + 1) * batch_size
    if batch_index == n_jobs - 1:
        end_index += n_jobs

    # check batch regular expression with white list urls
    res = dict()
    for index, regex in enumerate(regex_list[start_index: end_index]):
        hit = []
        for url in test_urls:
            if url_regex_path_match(regex, url):
                hit.append(url)
        if verbose:
            print "batch index %d\tsample index %d\thit url %d\t%s" \
            % (batch_index, index, len(hit), regex)
        if len(hit) != 0:
            res[regex] = hit
    return res


def make_prediction(urls, input_path, n_jobs=8, verbose = True):
    # load regex_list
    regex_list = load_regex_list(input_path)
    # pre-process
    path_domain_map = defaultdict(list)
    for url in urls:
        try:
            url_obj = urlparse.urlparse(url)
            if len(url_obj.path)>1:
                path_domain_map[url_obj.path].append(url_obj.hostname)
        except Exception:
            pass
    # prediction
    predict_res_list = Parallel(
        n_jobs=n_jobs)(
        delayed(_core_predict)(
            regex_list,
            path_domain_map.keys(),
            index,
            n_jobs,
            verbose) for index in range(n_jobs))
    # precess the result
    predict_malicious_domain = []
    predict_dict = defaultdict(list)
    for predict_res in predict_res_list:
        for regex in predict_res:
            for path in predict_res[regex]:
                predict_malicious_domain.extend(path_domain_map[path])
                predict_dict[regex].extend(path_domain_map[path]) 

    return list(set(predict_malicious_domain)), predict_dict


# check the extracted regular expression with white list url to avoid FP
def _check_performance(
        urls,
        regex_list,
        white_domain,
        trainset_domain,
        batch_index,
        n_jobs):

    # decides which batch of regular expression use to check
    batch_size = int(len(regex_list) / n_jobs)
    start_index = batch_index * batch_size
    end_index = (batch_index + 1) * batch_size
    if batch_index == n_jobs - 1:
        end_index += n_jobs
    # check batch regular expression with white list urls
    fp_list = []
    tp_list = []
    independent_detection_list = []
    fp_domain_map = defaultdict(set)
    detect_domain_map = defaultdict(set)
    for index, regex in enumerate(regex_list[start_index: end_index]):
        detection_cnt = 0
        tp_cnt = 0
        for url in urls:
            if url_regex_path_match(regex, url):
                detection_cnt += 1
                if urlparse.urlparse(url).hostname in trainset_domain:
                    tp_cnt += 1
                elif urlparse.urlparse(url).hostname in white_domain:
                    fp_domain_map[regex].add(urlparse.urlparse(url).hostname)
                else:
                    detect_domain_map[regex].add(urlparse.urlparse(url).hostname)
        independent_detection_cnt = detection_cnt - tp_cnt
        print "%40s\tindependent detection %d\ttp %d\tfp %d" % (regex, independent_detection_cnt, tp_cnt, len(fp_domain_map[regex]))
        fp_list.append(len(fp_domain_map[regex]))
        tp_list.append(tp_cnt)
        independent_detection_list.append(independent_detection_cnt)
    return [fp_list, tp_list, independent_detection_list, fp_domain_map, detect_domain_map]


def illegal_domain_evaluate_mp(urls, regex_list, white_domain, trainset_domain, result_file_path, n_jobs = 4):
    st_time = time.time()
    test_domain = set([urlparse.urlparse(_).hostname for _ in urls])
    white_domain = set([_ for _ in white_domain if _ in test_domain])
    logger.info("preprocess time consume:%f" %(time.time()-st_time))
    
    res_list = Parallel(n_jobs=n_jobs)(delayed(_check_performance)(
            urls, regex_list, white_domain, trainset_domain, batch_index, n_jobs) for batch_index in range(n_jobs))

    fp_list = []
    tp_list = []
    independent_detection_list = []
    fp_url_map = dict()
    detect_url_map = dict()
    for res in res_list:
        fp_list += res[0]
        tp_list += res[1]
        independent_detection_list += res[2]
        fp_url_map = dict(fp_url_map.items() + res[3].items())
        detect_url_map = dict(detect_url_map.items() + res[4].items())
    df = pd.DataFrame({"path": regex_list, "independent detection": independent_detection_list, "tp": tp_list, "fp": fp_list})
    dump_check_result(fp_list, independent_detection_list, regex_list, result_file_path)
    return df, fp_url_map, detect_url_map


def fp_regex_lookup(fp_domain_list, predict_dict):
    res = []
    predict_dict_reverse = defaultdict(set)
    for k, v in predict_dict.iteritems():
        for i in v:
            predict_dict_reverse[i].add(k)
    for i in fp_domain_list:
        res.extend(predict_dict_reverse[i])
    return res



