#! /usr/bin/env python
'''Run fstrim in chunks and sleep in between
'''
from __future__ import print_function, division
import os, sys, argparse, time, string, re, logging
from datetime import datetime
from random import random
from subprocess import check_output


LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(message)s'
formatter = logging.Formatter(LOG_FORMAT)
file_handler = logging.FileHandler('/var/log/nice_trim.log')
file_handler.setFormatter(formatter)
file_handler.setLevel('INFO')
log = logging.getLogger('nice_trim')
log.addHandler(file_handler)
log.setLevel(logging.DEBUG)


def get_trimable():
    '''Get dict mapping mount point to fs size for each mounted trimable FS'''
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
    df_out = [l for l in check_output(['df', '-B', '1', mount]).split('\n') if l != '']
    assert len(df_out) == 2
    dev, size, _, _, _, real_mount = df_out[1].split()
    if real_mount != mount:
        raise ValueError("Not a mountpoint: %s" % mount)
    return int(size)


hr_suff = {'' : 1,
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
    hr_str = hr_str.lower()
    if hr_str[-3] not in string.digits:
        suf_len = 3
    elif hr_str[-2] not in string.digits:
        suf_len = 2
    else:
        suf_len = 0
    num = int(hr_str[:-suf_len])
    suff = hr_str[-suf_len:]
    return num * hr_suff[suff]


def do_trim(offset, chunk_bytes, min_bytes, mount):
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


_prog_epilog = \
    '''Some space may be trimmed more than once due to the limitations of
    fstrim, and the reported amount of discarded bytes could be inflated.'''


def main(argv=sys.argv):
    parser = argparse.ArgumentParser(description=__doc__,
                                     epilog='Some space may be trimmed more than once ')
    parser.add_argument('mount', nargs='*', help="Mount points we are trimming")
    parser.add_argument('-a', '--all', action='store_true')
    parser.add_argument('-c', '--chunk-size', default='4GiB')
    parser.add_argument('-s', '--sleep-range', default='0.5,480')
    parser.add_argument('-m', '--min-extent', default='16MiB')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args(argv[1:])

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    if args.verbose:
        stream_handler.setLevel(logging.INFO)
    else:
        stream_handler.setLevel(logging.WARN)
    log.addHandler(stream_handler)

    if args.all:
        if len(args.mount) != 0:
            parser.error("No mounts should be given if --all is specified")
        mounts = get_trimable()
    else:
        mounts = {}
        for mount in args.mount:
            mounts[mount] = get_size(mount)

    min_sleep, max_sleep = args.sleep_range.split(',')
    min_sleep = float(min_sleep)
    max_sleep = float(max_sleep)
    sleep_range = max_sleep - min_sleep

    chunk_bytes = human_readable_to_bytes(args.chunk_size)
    min_bytes = human_readable_to_bytes(args.min_extent)
    # TODO: Should populate this from the FS allocation group size
    max_discard = human_readable_to_bytes('1TiB')

    for mount, fs_size in mounts.items():
        log.info("Processing mount point: %s" % mount)
        offset = 0
        discarded = 0
        last_chunk = max_discard
        while offset < fs_size:
            sleep_frac = min(float(last_chunk) / max_discard, 1.0)
            max_range = sleep_range * sleep_frac
            sleep_time = int((random() * max_range) + min_sleep)
            log.info("Sleeping for %d seconds" % sleep_time)
            time.sleep(sleep_time)
            log.info("Running the trim command with offset: %d" % offset)
            start_time = datetime.now()
            n_disc = do_trim(offset, chunk_bytes, min_bytes, mount)
            trim_time = datetime.now() - start_time
            if n_disc > chunk_bytes:
                log.info("Hit large free extent, moving offset forward %d bytes" % n_disc)
                offset += n_disc
                last_chunk = n_disc
            else:
                offset += chunk_bytes
                last_chunk = chunk_bytes
            discarded += n_disc
            log.info("Trim took: %s" % trim_time)
        log.info("Discarded roughly %d bytes" % discarded)


if __name__ == '__main__':
    sys.exit(main())
