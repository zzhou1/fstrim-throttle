#! /usr/bin/env python
'''
A wrapper of fstrim. It runs fstrim in chunks and sleep in between.
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
from subprocess import check_output, CalledProcessError, STDOUT

locale.setlocale(locale.LC_ALL, 'en_US')

def get_devpath_from_mountpoint(mountpoint, log):
    'helper function'
    log.debug("mountpoint = '%s'", mountpoint)
    try:
        devpath = check_output(['findmnt', '-T', mountpoint, '-o', 'SOURCE'])
    except CalledProcessError:
        # ignore any invalid mountpoint, including '[SWAP]'
        return ''

    # split and collect the second line only
    sec_line_as_devpath = devpath.split('\n')[1]
    log.debug("devpath = '%s'", sec_line_as_devpath)
    # deal with the btrfs snapshot form in mountpoint
    res = sec_line_as_devpath.split('[', 1)
    if len(res) > 1:
        log.debug("'%s' for mount point '%s' is stripped as '%s'",
                  sec_line_as_devpath, mountpoint, res[0])

    log.debug("devpath = '%s'", res[0])
    return res[0]

def get_trimmable(log):
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
        devpath = get_devpath_from_mountpoint(blk_info['MOUNTPOINT'], log)
        if devpath == '':
            continue
        result[blk_info['MOUNTPOINT']] = [devpath, int(blk_info['SIZE'])]
    return result


def get_devpath_fssize(mount):
    'helper function'
    df_out = [l for l in check_output(['df', '-B', '1', mount]).split('\n') if l != '']
    assert len(df_out) == 2
    dev, size, _, _, _, real_mount = df_out[1].split()
    #if real_mount != mount:
    #    raise ValueError("Not a mountpoint: %s" % mount)
    return [real_mount, dev, int(size)]

# devpath is needed to detect the filesystem block size
def round_up_to_fs_block_size(size, devpath, log):
    '''
    helper function to round up --chunk-size if it is too small, to avoid
    the error report from fstrim.
    '''
    log.debug('devpath = %s', devpath)
    try:
        blockdev_out = check_output(['blockdev', '--getbsz', devpath]).strip()
    except CalledProcessError:
        return -1
    log.debug('blockdev fs block size = %s', blockdev_out)
    fs_block_size = max(int(size), int(blockdev_out))
    if size < fs_block_size:
        log.info("[chunk_size = %s] get rounded up to the filesystem blocksize",
                 fs_block_size)
    return fs_block_size

HR_STUFF = {'' : 1,
            'k' : 1024,
            'm' : 1024*1024,
            'g' : 1024**3,
            't' : 1024**4,
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
    if len(hr_str) >= 4 and hr_str[-4] not in string.digits:
        return -1
    if len(hr_str) >= 3 and hr_str[-3] not in string.digits:
        suf_len = 3
    elif len(hr_str) >= 2 and hr_str[-2] not in string.digits:
        suf_len = 2
    elif len(hr_str) >= 1 and hr_str[-1] not in string.digits:
        suf_len = 1
    else:
        suf_len = 0
    if len(hr_str) == suf_len:
        return -1
    if suf_len < 1:
        return int(hr_str)
    num = int(hr_str[:-suf_len])
    suff = hr_str[-suf_len:]
    if suff in HR_STUFF:
        return num * HR_STUFF[suff]
    return -1

def fmt(num, flag_for_bytes):
    'helper function'
    if flag_for_bytes:
        return num
    return locale.format("%d", num, grouping=True)

def do_trim(offset, args, mount):
    'helper function'
    log = logging.getLogger('nice_trim')
    try:
        fst_out = check_output(['ionice', '-c', 'idle',
                                'fstrim', '-v', '-o', str(offset),
                                '-l', str(args.chunk_size), '-m',
                                str(args.min_extent), mount],
                               stderr=STDOUT)
    except CalledProcessError as e:
        log.info("%s", e.output.strip())
        return -1

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
It intends to throttle fstrim and leave the room for the normal WRITE request
IO to get through to the backend block device. A plain fstrim might initiate
DISCARD requests, disturb ongoing IO too much, cause the long freeze, and harm
the critical service.

The human readable format includes K/KiB, M/MiB, G/GiB, T/TiB, KB, MB, GB, TB.
'''

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
                                     #formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     formatter_class=argparse.RawDescriptionHelpFormatter
                                    )
    parser.add_argument('mount', nargs='*', help=argparse.SUPPRESS)
    parser.add_argument('-a', '--all', action='store_true',
                        help="auto-detect all trimmable. This overrides any mount point")
    parser.add_argument('-b', '--bytes', action='store_true',
                        help="print SIZE in bytes rather than in human readable format")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debugging information')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='duplicate the log INFO to STDOUT as well')

    default_chunk = '4GiB'
    default_sleep = '0.5'
    default_min = '16MiB'
    default_log_file = '/var/log/nice_trim.log'
    info_option_desc = \
'''
mount_point    mount points we are trimming. Internally, fstrim will convert any
               directory path to the corresponding mount point or block device.
-c, --chunk-size <bytes>
               to search for free blocks to discard. kernel will internally
               round it up to a multiple of the filesystem block size. Also
               this tool will round it up to the filesystem block size to avoid
               fstrim error report if too small (default: %s)
-m, --min-extent <bytes>
               the minimum contiguous free range to discard. kernel will
               internally round it up to a multiple of the filesystem block
               size. Zero is to discard every free block (default: %s)
-s, --sleep-range <min,max> in seconds (default: %s)
               MIN would give a reasonable sleep between fstrim chunks.
               MAX would give a bigger random sleep to avoid all virtual machines or
               physical machines to stress the storage system all at once.
               eg. a random range '0.5,600'
               eg. 0.5 is equal to '0.5,0.5'
-l, --log-file <path> (default: %s)
''' % (default_chunk, default_min, default_sleep, default_log_file)
    parser.add_argument_group(title='information options',
                              description=info_option_desc)
    parser.add_argument('-c', '--chunk-size', default=default_chunk,
                        help=argparse.SUPPRESS)
    parser.add_argument('-s', '--sleep-range', default='0.5',
                        help=argparse.SUPPRESS)
    parser.add_argument('-m', '--min-extent', default='16MiB',
                        help=argparse.SUPPRESS)
    parser.add_argument('-l', '--log-file', nargs='?', type=str,
                        default=default_log_file,
                        help=argparse.SUPPRESS)

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
        parser.error("no mountpoint should be given if --all is specified")

    if not args.all and not args.mount:
        parser.error("no mountpoint specified")

    log = setup_log_file(args)

    args.chunk_size = human_readable_to_bytes(args.chunk_size)
    if args.chunk_size < 0:
        parser.error('incorrect human readable format in --chunk-size option')
    log.info("[chunk_size = %s] to search for free block to discard",
             fmt(args.chunk_size, args.bytes))

    args.min_extent = human_readable_to_bytes(args.min_extent)
    if args.min_extent < 0:
        parser.error('incorrect human readable format in --min_extent option')
    log.info("[min_extent = %s] min contiguous free range to discard",
             fmt(args.min_extent, args.bytes))

    return args, log

def setup_log_file(args):
    'helper function'
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    file_handler = logging.FileHandler(args.log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel('INFO')
    log = logging.getLogger('nice_trim')
    log.addHandler(file_handler)
    log.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    if args.debug:
        stream_handler.setLevel(logging.DEBUG)
    elif args.verbose:
        stream_handler.setLevel(logging.INFO)
    else:
        stream_handler.setLevel(logging.WARN)
    log.addHandler(stream_handler)
    return log

def main():
    'main function'
    args, log = cli_parser()

    tmp = args.sleep_range.split(',')
    if len(tmp) == 1:
        min_sleep = tmp[0]
        max_sleep = min_sleep
    else:  # len(tmp) == 2:
        min_sleep, max_sleep = tmp

    if args.all:
        mounts = get_trimmable(log)
    else:
        mounts = {}
        for mount in args.mount:
            real_mount, devpath, fs_size = get_devpath_fssize(mount)
            mounts[real_mount] = [devpath, fs_size]
            log.debug("mountpoint = '%s', devpath = '%s', fs_size = '%s'",
                      real_mount, devpath, fs_size)

    min_sleep = float(min_sleep)
    max_sleep = float(max_sleep)
    sleep_range = max_sleep - min_sleep
    log.info("[min, max = %s, %s] sleep in seconds", min_sleep, max_sleep)

    # TODO: Should populate this from the FS allocation group size
    max_discard = human_readable_to_bytes('1TiB')

    for mount, devpath_size in mounts.items():
        devpath, fs_size = devpath_size

        # eg. LUKS is not trimmable
        args.chunk_size = round_up_to_fs_block_size(args.chunk_size,
                                                    devpath, log)
        if args.chunk_size < 0:
            log.info('"%s" is not trimmable, really, and is ignored', mount)
            continue

        log.info("Processing mount point '%s' at '%s'", mount, devpath)
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
                     fmt(offset, args.bytes))
            start_time = datetime.now()

            # eg. [SWAP], /dev/efi are not trimmable
            n_disc = do_trim(offset, args, mount)
            if n_disc < 0:
                log.info('"%s" is not trimmable, really, and is ignored', mount)
                break

            trim_time = datetime.now() - start_time
            if n_disc > args.chunk_size:
                log.info("Hit large free extent, moving offset forward %s bytes",
                         fmt(n_disc, args.bytes))
                offset += n_disc
                last_chunk = n_disc
            else:
                offset += args.chunk_size
                last_chunk = args.chunk_size
            discarded += n_disc
            log.info("Trim took: %s", trim_time)
        if n_disc > 0:
            log.info("Discarded roughly %s bytes", fmt(discarded, args.bytes))


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(-1)
