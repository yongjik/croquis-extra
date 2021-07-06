#!/usr/bin/env python3
#
# A simple script to generate example temperature data from NOAA raw data.
# You can download the raw data from:
#   https://www.ncei.noaa.gov/data/global-hourly/archive/csv/2020.tar.gz
#
# We do a very minimal filtering by discarding all data outside -100..100 °C.
# (The filtered data still contains quite a few bogus data points, as you can
# see in the visualization.)
#
# How to use:
#   ./gen_temperature_dat.py 2020.tar.gz test.dat test.npz
#
#   test.dat : intermediate data file as text (one line per item).
#   tsst.npz : the final result file
#
# This script may take ~30 min to run.

import csv, io, sys, re, tarfile, traceback

import ciso8601
import numpy as np

# MAX_FILES = 20
MAX_FILES = 1000000

class TarFileProcessor(object):
    def __init__(self, tar_filename, txtfile):
        self.tar_filename = tar_filename
        self.txtfile = txtfile

    def run(self):
        self.file_idx = -1
        with tarfile.open(self.tar_filename) as tf, \
             open(self.txtfile, 'wt') as outf:
            while True:
                self.info = tf.next()
                if self.info is None: break
                self.file_idx += 1
                if self.file_idx > MAX_FILES: return

                print(f'{self.file_idx}: processing {self.info.name} ...',
                      file=sys.stderr)

                with tf.extractfile(self.info) as f:
                    try:
                        self.process_tar_file(f, outf)
                    except KeyboardInterrupt:
                        raise
                    except:
                        print('Parse error on {self.file_idx}: ignoring file ...',
                              file=sys.stderr)
                        traceback.print_exc()

    # Process one file stored in the tar archive.
    def process_tar_file(self, f, outf):
        with io.TextIOWrapper(f, 'utf-8') as textf:
            reader = csv.DictReader(textf)
            first = True
            for row in reader:
                if first:
                    name = row['NAME']
                    lat = row['LATITUDE']
                    lon = row['LONGITUDE']
                    print(file=outf)
                    print(f'>>> {self.file_idx} {self.info.name} {lat} {lon} "{name}"', file=outf)
                    first = False

                try:
                    ts = row['DATE']
                    ts = ciso8601.parse_datetime(ts + 'Z').timestamp()
                    ts = round(ts)
                    temp = row['TMP']

                    # Remove "quality code" letters if present.
                    # cf. https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf
                    if 'A' <= temp[-1] <= 'Z': temp = temp[:-1]

                    temp = float(temp.replace(',', '.')) * 0.1  # Celsius

                    if -100 < temp < 100:
                        print(self.file_idx, ts, '%+.2f' % temp, file=outf)

                except KeyboardInterrupt:
                    raise
                except:
                    print('Parse error on {self.file_idx}: ignoring ...', file=sys.stderr)
                    traceback.print_exc()

def make_np_dat(txtfile, outfile):
    names = []
    coords = []
    start_idxs = []
    timestamps = []
    temperatures = []
    with open(txtfile) as f:
        for line in f:
            if line == '\n': continue

            m = re.match(r'>>> \d+ \S+ ([-+0-9.]+) ([-+0-9.]+) "(.*)"', line)
            if m:
                names.append(m.group(3))
                coords.append((float(m.group(1)), float(m.group(2))))
                start_idxs.append(len(timestamps))
                continue

            file_idx, ts, temp = line.split()
            assert int(file_idx) == len(start_idxs) - 1
            timestamps.append(int(ts))
            temperatures.append(float(temp))

    np.savez(outfile,
             names=names,
             coords=np.array(coords, dtype=np.float32),
             start_idxs=np.array(start_idxs, dtype=np.int32),
             timestamps=np.array(timestamps, dtype=np.int32),
             temperatures=np.array(temperatures, dtype=np.float32))

TarFileProcessor(sys.argv[1], sys.argv[2]).run()
make_np_dat(sys.argv[2], sys.argv[3])

# How to use:
#   A = np.load('test.npz')
#   names = A['names']
#   timestamps = A['timestamps']
#   ...
