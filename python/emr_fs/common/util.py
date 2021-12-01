#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import re
import json

from datetime import datetime
from emr_fs import client, feature


def exec_command(cmd: str, timeout=10) -> str:
    try:
        output_bytes = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, timeout=timeout)
    except subprocess.CalledProcessError as err:
        result = err.output.decode('utf-8')
        raise err
    result = output_bytes.decode('utf-8')
    return result


def pares_features(feature_keys={}):
    features = []
    for key, value in feature_keys:
        feature = Feature(key,value)
        features.append(feature)
    return features

def get_timestamp_from_date_string(input_date):
    date_format_patterns = {
        r"^([0-9]{4})([0-9]{2})([0-9]{2})$": "%Y%m%d",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M%S",
    }
    input_date = (
        input_date.replace("/", "").replace("-", "").replace(" ", "").replace(":", "")
    )

    date_format = None
    for pattern in date_format_patterns:
        date_format_pattern = re.match(pattern, input_date)
        if date_format_pattern:
            date_format = date_format_patterns[pattern]
            break

    if date_format is None:
        raise ValueError(
            "Unable to identify format of the provided date value : " + input_date
        )

    return int(float(datetime.strptime(input_date, date_format).timestamp()) * 1000)


def get_hudi_datestr_from_timestamp(timestamp):
    date_obj = datetime.fromtimestamp(timestamp / 1000)
    date_str = date_obj.strftime("%Y%m%d%H%M%S")
    return date_str




