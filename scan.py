import re
import os
import sys
from collections import defaultdict
import json
from typing import Dict
import pandas as pd
from vika import Vika
from vika.exceptions import RecordDoesNotExist

vika = Vika("usk1AZuHPwIdgDEOgbtf4re")
vika.set_api_base("https://ku.baidu-int.com/")
datasheet = vika.datasheet("dstcvk6ZRbLC21zjod", field_key="id")

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

def parse_file(filename) -> Dict[str, str]:
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
    error_categories = [py3_error_category, mac_error_category]

    error_category = defaultdict(set)
    for d in error_categories:
        for unit_test, categories in d.items():
            error_category[unit_test].update(categories)
    df = pd.DataFrame.from_dict(error_category, orient='index')
    df = df.rename(lambda x: '错误{}'.format(x), axis='columns')
    df.sort_values(by=['错误0'], inplace=True)
    df.to_csv(os.path.join(path, "test2category.csv"))

    error_to_unittest = defaultdict(set)
    for unit_test, categories in error_category.items():
        for category in categories:
            error_to_unittest[category].add(unit_test)
    df_e2u = pd.DataFrame.from_dict(error_to_unittest, orient='index')
    df_e2u = df_e2u.rename(lambda x: '{}'.format(x), axis='columns')
    df_e2u['相关单测个数'] = df_e2u.count(axis=1)
    cols = df_e2u.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df_e2u = df_e2u[cols]
    df_e2u.sort_index(ascending=False, inplace=True)
    df.to_csv(os.path.join(path, "category2test.csv"))
    
    excel_path = os.path.join(path, 'summary.xlsx')
    with pd.ExcelWriter(excel_path) as writer:
        df.to_excel(writer, sheet_name='test2category')
        df_e2u.to_excel(writer, sheet_name='category2test')

def compare_two_error_category(err_cat1, err_cat2) -> None:
    intersection = set(err_cat1.keys()).intersection(err_cat2.keys())
    resolved = set(err_cat1.keys()).difference(err_cat2.keys())
    occured = set(err_cat2.keys()).difference(err_cat1.keys())
    print(f'following {len(resolved)} unittests resolved...')
    for unit in resolved:
        print(unit)
    print(f'following {len(occured)} unittests failed recently...')
    for unit in occured:
        print(unit)
    
    updated = set()
    for unit in intersection:
        if (err_cat1[unit] == err_cat2[unit]):
            continue
        updated.add(unit)
    print(f'following {len(updated)} unittests\' cause updated...')
    for unit in updated:
        print(''.join(['>' for i in range(20)]))
        print(err_cat1[unit])
        print(''.join(['=' for i in range(20)]))
        print(err_cat2[unit])
        print(''.join(['<' for i in range(20)]))

def compare_two_file(old_file, new_file):
    err_cat1 = parse_file(old_file)
    err_cat2 = parse_file(new_file)

    compare_two_error_category(err_cat1, err_cat2)


def compare_two_directory(dir1, dir2):
    d1 = [parse_file(os.path.join(dir1, "py3.log")), parse_file(os.path.join(dir1, "mac.log"))]
    err_cat1 = defaultdict(set)
    for d in d1:
        for unit_test, categories in d.items():
            err_cat1[unit_test].update(categories)

    d2 = [parse_file(os.path.join(dir2, "py3.log")), parse_file(os.path.join(dir2, "mac.log"))]
    err_cat2 = defaultdict(set)
    for d in d2:
        for unit_test, categories in d.items():
            err_cat2[unit_test].update(categories)
    compare_two_error_category(err_cat1, err_cat2)

def get_new_ir_white_list() -> set:
    with open('new_ir_white_list', 'r') as f:
        lines = f.readlines()
        lines = [x.strip() for x in lines]
        lines = filter(lambda x: len(x.strip()) > 0, lines)
        return set(lines)

def get_current_list(datasheet, fm: Dict[str, str]) -> set:
    records = datasheet.records.all(fields=[fm[x] for x in ["单测名称"]])
    cl = set()
    for record in records:
        cl.add(record.json()[fm['单测名称']])
    return cl

def get_fields_mapping(datasheet) -> Dict[str, str]:
    fields = datasheet.fields.all()
    fm = {}
    for field in fields:
        j = field.json()
        j = json.loads(j)
        fm[j['name']] = j['id'] 
    return fm

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def update_from_err_cat(err_cat, all_list):
    pass

def update_new_records(cl:set, wl:set, fm):
    diff = wl.difference(cl)
    all_records = []
    for unit in diff:
        try:
            is_existed = datasheet.records.get(单测名称=unit)
        except RecordDoesNotExist as e:
            print(f'not found {unit}')
            r = {
                fm["单测名称"]: unit,
                fm["当前状态"]: "已修复(自动)"
            }
            all_records.append(r)
    for chunk in chunks(all_records, 10):
        records = datasheet.records.bulk_create(chunk)

def update_white_list():
    wl = get_new_ir_white_list()
    fm = get_fields_mapping(datasheet)
    cl = get_current_list(datasheet, fm)
    update_new_records(cl, wl, fm)

def update_routine(path, update=False):
    fm = get_fields_mapping(datasheet)
    cl = get_current_list(datasheet, fm)
    err_cnt = parse_file(path)

    freshly_failed = set(err_cnt.keys()).difference(cl)
    print(f'following {len(freshly_failed)} unittests freshly failed...')
    for unit in freshly_failed:
        print(unit)

    resolved = set()
    failed = set()
    for unit in cl:
        row = datasheet.records.get(单测名称=unit)
        r = row.json()
        state = "undefined" if "当前状态" not in r else r["当前状态"]
        if unit not in err_cnt and state is not "已修复(自动)":
            resolved.add(unit)
            if update:
                row.update({fm["当前状态"]: "已修复(自动)"})
            continue
        if unit in err_cnt and state is "已修复(自动)":
            failed.add(unit)
            if update:
                row.update({fm["当前状态"]: "待分析"})
            continue
        elif unit in err_cnt:
            if update:
                record = {}
                error_keys = [ fm[f"错误{i}"] for i in range(9)]
                for ek, cat in zip(error_keys, err_cnt[unit]):
                    record[ek] = cat
                row.update(record)
            continue
    print(f'following {len(resolved)} unittests resolved...')
    for unit in resolved:
        print(unit)
    print(f'following {len(failed)} unittests failed recently...')
    for unit in failed:
        print(unit)



if __name__ == '__main__': 
    # parse_mac_and_py3(sys.argv[1])
    # compare_two_directory("log/0629", "log/0710")
    # compare_two_file("log/0629/py3.log", "log/0706/py3.log")
    update_routine("log/0712/py3.log", update=True)