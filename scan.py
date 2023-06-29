import re
import os
import sys
from collections import defaultdict
import pandas as pd

pattern_extract_robust_unit_test = r'\d+/\d+ Test\s+#\d+: (?P<unit_test>.*) \.*\**Failed'
pattern_extract_robust_unit_test = re.compile(pattern_extract_robust_unit_test)
pattern_extract_unit_test = r'ERROR: .* \((?P<unit_test>.*)\.(?P<test_name>.*)\)'
pattern_extract_unit_test = re.compile(pattern_extract_unit_test)
pattern_split_primary = r'======================================================================'
pattern_split_primary = re.compile(pattern_split_primary)
pattern_split_secondary = r'----------------------------------------------------------------------'
pattern_split_secondary = re.compile(pattern_split_secondary)
pattern_msg_of_ir_error = re.compile(r':\s+Error occured at:')
pattern_unittest_start = r'\s+Start\s+\d+: .*\n'
pattern_unittest_start = re.compile(pattern_unittest_start)
pattern_unittest_end = r'\d+/\d+ Test\s+#\d+: .* \.*'
pattern_unittest_end = re.compile(pattern_unittest_end)
pattern_filter_time_signature = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s')
pattern_msg_of_paddle_enforce = re.compile(r'Error Message Summary:')
pattern_split_of_paddle_enfoce = re.compile(r'----------------------')
pattern_assertion_error_of_numpy = re.compile(r'AssertionError: ')
pattern_python_trace_back = re.compile(r'Traceback (most recent call last):')
pattern_segfault_for_mac3 = re.compile(r'Segmentation fault')
pattern_filter_digits = re.compile(r'\d+')
pattern_filter_hex_digits_for_segemetation_fault = re.compile(r'0[xX][0-9a-fA-F]+')

def parse_to_get_full_traceback(log, logs):
    trace_backs = []
     
    while log is not None \
        and pattern_unittest_start.search(log) is None \
        and pattern_unittest_end.search(log) is None:
        trace_backs.append(log)
        log = next(logs, None)
    return trace_backs

def parse_ir_enfoce_error(trace_backs):
    logs = iter(trace_backs)
    log = next(logs, None)
    error_category = ''
    categories = set()
    while log is not None:
        ret = pattern_split_primary.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        ret = pattern_extract_unit_test.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        unit_test = ret['unit_test']
        test_name = ret['test_name']
        ret = pattern_split_secondary.search(log)
        log = next(logs, None)
        if ret is None:
            continue
            
        while log is not None and pattern_msg_of_ir_error.search(log) is None:
            log = next(logs, None)
        if log is None:
            return set()
        error_category += log
        error_category += next(logs, '')
        error_category = pattern_filter_digits.sub('', error_category)
        categories.add(error_category)
        error_category = ''
    return categories

def parse_paddle_enforce_error(trace_backs):
    logs = iter(trace_backs)
    log = next(logs, None)
    
    categories = set()
    while log is not None:
        ret = pattern_msg_of_paddle_enforce.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        ret = pattern_split_of_paddle_enfoce.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        error_category = ''
        while log is not None and len(log.strip()) > 0:
            log = pattern_filter_hex_digits_for_segemetation_fault.sub('', log)
            log = pattern_filter_digits.sub('', log)
            error_category += log
            log = next(logs, None)
        if log is None:
            return set()
        categories.add(error_category)
    return categories

def parse_assert_error(trace_backs):
    logs = iter(trace_backs)
    log = next(logs, None)
    
    categories = set()
    while log is not None:
        ret = pattern_assertion_error_of_numpy.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        categories.add('精度问题')
    return categories

def parse_python_traceback(trace_backs):
    logs = iter(trace_backs)
    log = next(logs, None)
    error_category = ''
    categories = set()
    while log is not None:
        ret = pattern_split_primary.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        ret = pattern_extract_unit_test.search(log)
        log = next(logs, None)
        if ret is None:
            continue
        unit_test = ret['unit_test']
        test_name = ret['test_name']
        ret = pattern_split_secondary.search(log)
        log = next(logs, None)
        while ret is None and log is not None:
            ret = pattern_split_secondary.search(log)
            log = next(logs, None)
        if ret is None:
            continue
            
        while log is not None and len(log.strip()) > 0:
            error_category = log
            log = next(logs, None)
        if log is None:
            return set()
        
        categories.add(error_category)
        error_category = ''
    return categories

def parse_segmentation_fault_for_mac3(trace_backs):
    logs = iter(trace_backs)
    log = next(logs, None)
    categories = set()
    while log is not None and pattern_segfault_for_mac3.search(log) is None:
        log = next(logs, None)
    categories.add("Segmentation fault")
    return categories

def parse_file(filename):
    with open(filename, mode='r') as f:
        lines = f.readlines()
        lines = [pattern_filter_time_signature.sub("", log) for log in lines]
        unit_tests = set()
        unit_tests_category = {}

        logs = iter(lines)
        log = next(logs, None)

        while log is not None:
            ret = pattern_extract_robust_unit_test.search(log)
            log = next(logs, None)
            if ret is None:
                continue
            new_unit_test = ret['unit_test']
            unit_tests.add(new_unit_test)

            trace_backs = parse_to_get_full_traceback(log, logs)
            if len(trace_backs) == 0:
                print('unittest %s has no trackback')
                continue
            categories = parse_ir_enfoce_error(trace_backs)
            if len(categories) > 0:
                unit_tests_category[new_unit_test] = categories
                continue
            categories = parse_paddle_enforce_error(trace_backs)
            if len(categories) > 0:
                unit_tests_category[new_unit_test] = categories
                continue
            categories = parse_python_traceback(trace_backs)
            if len(categories) > 0:
                unit_tests_category[new_unit_test] = categories
                continue
            categories = parse_assert_error(trace_backs)
            if len(categories) > 0:
                unit_tests_category[new_unit_test] = categories
                continue
            categories = parse_segmentation_fault_for_mac3(trace_backs)
            if len(categories) > 0:
                unit_tests_category[new_unit_test] = categories
                continue
        
        if len(set(unit_tests)) > len(set(unit_tests_category)):
            print(filename)
            print(set(unit_tests) - set(unit_tests_category.keys()))
        
        return unit_tests_category

def parse_mac_and_py3(path):
    mac_path = os.path.join(path, 'mac.log')
    py3_path = os.path.join(path, 'py3.log')

    mac_error_category = parse_file(mac_path)
    py3_error_category = parse_file(py3_path)

    error_category = defaultdict(set)
    for d in (mac_error_category, py3_error_category):
        for unit_test, categories in d.items():
            error_category[unit_test].update(categories)
    df = pd.DataFrame.from_dict(error_category, orient='index')
    df.to_csv("{}_test2category.csv".format(path))

    error_to_unittest = defaultdict(set)
    for unit_test, categories in error_category.items():
        for category in categories:
            error_to_unittest[category].add(unit_test)
    df_e2u = pd.DataFrame.from_dict(error_to_unittest, orient='index')
    df_e2u.to_csv("{}_category2test.csv".format(path))
    
    with pd.ExcelWriter('{}.xlsx'.format(path)) as writer:  
        df.to_excel(writer, sheet_name='test2category')
        df_e2u.to_excel(writer, sheet_name='category2test')

if __name__ == '__main__': 
    parse_mac_and_py3(sys.argv[1])