#! /usr/bin/env python
'''
A wrap of fstrim. It runs fstrim in chunks and sleep in between.
'''
from __future__ import print_function, division
import os
import sys
import argparse
import time
import string
import re
import logging
import locale
from datetime import datetime
from random import random
from subprocess import check_output

locale.setlocale(locale.LC_ALL, 'en_US')

LOGFILE = '/var/log/nice_trim.log'

def get_trimable():
    'helper function'
    result = {}
    lsblk_out = check_output(['lsblk', '-POb'])
    lsblk_out = [l.strip() for l in lsblk_out.split('\n') if l.strip() != '']
    for out_line in lsblk_out:
        blk_info = {}
        for match in re.finditer('([A-Z\-\:]+)="([^"]*)"', out_line):
            blk_key, blk_val = match.groups()
            blk_info[blk_key] = blk_val
        if blk_info['MOUNTPOINT'] == '':
            continue
        if blk_info['DISC-GRAN'] == '0':
            continue
        if blk_info['RO'] == '1':
            continue
        result[blk_info['MOUNTPOINT']] = int(blk_info['SIZE'])
    return result


def get_size(mount):
    'helper function'
    df_out = [l for l in check_output(['df', '-B', '1', mount]).split('\n') if l != '']
    assert len(df_out) == 2
    _dev, size, _, _, _, real_mount = df_out[1].split()
    if real_mount != mount:
        raise ValueError("Not a mountpoint: %s" % mount)
    return int(size)


HR_STUFF = {'' : 1,
            'kib' : 1024,
            'mib' : 1024*1024,
            'gib' : 1024**3,
            'tib' : 1024**4,
            'kb' : 1000,
            'mb' : 1000000,
            'gb' : 1000000000,
            'tb' : 1000000000000,
           }


def human_readable_to_bytes(hr_str):
    'helper function'
    hr_str = hr_str.lower()
    if hr_str[-3] not in string.digits:
        suf_len = 3
    elif hr_str[-2] not in string.digits:
        suf_len = 2
    else:
        suf_len = 0
    num = int(hr_str[:-suf_len])
    suff = hr_str[-suf_len:]
    return num * HR_STUFF[suff]

def fmt(num, flag_for_machine):
    'helper function'
    if flag_for_machine:
        return num
    return locale.format("%d", num, grouping=True)

def do_trim(offset, chunk_bytes, min_bytes, mount):
    'helper function'
    fst_out = check_output(['ionice', '-c', 'idle',
                            'fstrim', '-v', '-o', str(offset),
                            '-l', str(chunk_bytes), '-m', str(min_bytes),
                            mount])
    fst_out = [l.strip() for l in fst_out.split('\n') if l.strip() != '']
    assert len(fst_out) == 1
    fst_out = fst_out[0]
    l_idx = fst_out.find('(') + 1
    r_idx = fst_out.find(')')
    substr = fst_out[l_idx:r_idx]
    b_str, test = substr.split()
    assert test == 'bytes'
    return int(b_str)

_DESC = __doc__ + '''
    It intends to throttle fstrim and leave some free IO bandwith to allow the
    normal WRITE requests get through to the backend block device. A plain
    fstrim might initiate intensive DISCARD requests, saturate IO, cause the
    long freeze, and harm the critical service.

    The human readable format includes KiB, MiB, GiB, TiB, KB, MB, GB, and TB.
    The log file is at ''' + LOGFILE

#_prog_epilog = \
#    '''
#    Some space may be trimmed more than once due to the limitations of
#    fstrim, and the reported amount of discarded bytes could be inflated.

def cli_parser():
    'helper function'
    parser = argparse.ArgumentParser(description=_DESC,
                                     #epilog='Some space may be trimmed more than once ',
                                     epilog='Example: ' +
                                     os.path.basename(__file__) + ' -a',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                    )
    parser.add_argument('mount', nargs='*', help="Mount points we are trimming")
    parser.add_argument('-a', '--all', action='store_true',
                        help="this overrides any mount point")
    parser.add_argument('-c', '--chunk-size', default='4GiB',
                        help="The number of bytes to search for free blocks to discard.")
    parser.add_argument('-s', '--sleep-range', default='0.5',
                        help="in seconds, eg. 0.5, or a random range '0.5,480' ")
    parser.add_argument('-m', '--min-extent', default='16MiB',
                        help='''Minimum contiguous free range to discard, in
                        bytes, which is rounded up to the filesystem block
                        size.''')
    parser.add_argument('-n', '--for-machine', action='store_true',
                        help="no thousands separators for bytes in the output")
    parser.add_argument('-v', '--verbose', action='store_true')

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args(sys.argv[1:])

    if os.getuid() != 0:
        parser.error("please run as a root user. Refer to -h | --help")

    tmp = args.sleep_range.split(',')
    if len(tmp) > 2:
        parser.error("incorrect --sleep_range format")

    if args.all and args.mount:
        parser.error("No mounts should be given if --all is specified")

    return parser.parse_args(sys.argv[1:])

def setup_log_file(args, logfile):
    'helper function'
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    file_handler.setLevel('INFO')
    log = logging.getLogger('nice_trim')
    log.addHandler(file_handler)
    log.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    if args.verbose:
        stream_handler.setLevel(logging.INFO)
    else:
        stream_handler.setLevel(logging.WARN)
    log.addHandler(stream_handler)
    return log

def main(logfile):

    args = cli_parser()
    log = setup_log_file(args, logfile)

    tmp = args.sleep_range.split(',')
    if len(tmp) == 1:
        min_sleep = tmp[0]
        max_sleep = min_sleep
    else:  # len(tmp) == 2:
        min_sleep, max_sleep = tmp

    if args.all:
        mounts = get_trimable()
    else:
        mounts = {}
        for mount in args.mount:
            mounts[mount] = get_size(mount)

    chunk_bytes = human_readable_to_bytes(args.chunk_size)
    log.info("[chunk_size] = [%s], to search for free block to discard", args.chunk_size)

    min_bytes = human_readable_to_bytes(args.min_extent)
    log.info("[min_extent] = [%s], min contiguous free range to discard", args.min_extent)

    min_sleep = float(min_sleep)
    max_sleep = float(max_sleep)
    sleep_range = max_sleep - min_sleep
    log.info("[min_sleep, max_sleep] = %s, in seconds", [min_sleep, max_sleep])

    # TODO: Should populate this from the FS allocation group size
    #max_discard = human_readable_to_bytes('1TiB')
    max_discard = chunk_bytes

    for mount, fs_size in mounts.items():
        log.info("Processing mount point: %s", mount)
        offset = 0
        discarded = 0
        last_chunk = max_discard
        while offset < fs_size:
            sleep_frac = min(float(last_chunk) / max_discard, 1.0)
            max_range = sleep_range * sleep_frac
            sleep_time = random() * max_range + min_sleep
            log.info("Sleeping for %.2f seconds", sleep_time)
            time.sleep(sleep_time)
            log.info("Running the trim command with offset: %s",
                     fmt(offset, args.for_machine))
            start_time = datetime.now()
            n_disc = do_trim(offset, chunk_bytes, min_bytes, mount)
            trim_time = datetime.now() - start_time
            if n_disc > chunk_bytes:
                log.info("Hit large free extent, moving offset forward %s bytes",
                         fmt(n_disc, args.for_machine))
                offset += n_disc
                last_chunk = n_disc
            else:
                offset += chunk_bytes
                last_chunk = chunk_bytes
            discarded += n_disc
            log.info("Trim took: %s", trim_time)
        log.info("Discarded roughly %s bytes", fmt(discarded, args.for_machine))


if __name__ == '__main__':
    sys.exit(main(LOGFILE))
