import json
import logging
import re
import time
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import time

from flask import Flask, Response, jsonify, request


class BackendServer:
    def __init__(self):
        self._app = Flask(__name__)
        self._app.add_url_rule('/up', 'up', self.up)
        self._app.add_url_rule('/get_tick', 'get_tick', self.return_tick)
        self._app.add_url_rule('/get_timeline', 'get_timeline', self.get_timeline)

        self.time_series = pd.read_csv("time_series.csv")
        self.time_series["unix"] = pd.to_datetime(self.time_series["time"]).astype(np.int64) // 10**6
        self.counter=0
        self.max_counter=96

        # self._app.register_error_handler(Exception, self.handle_error_generic)
        # self._app.register_error_handler(HTTPException, self.handle_error_http)

        self.timeline = self._init_timeline()

        self.time_series.drop(columns=["Unnamed: 0", "unix"], inplace=True)
        self.time_series["time"] = pd.to_datetime(self.time_series["time"])


    def _init_timeline(self):
        timeline = []
        last_val = 0
        last_name = self.time_series.iloc[0]["name"]
        cur_active = False
        last_time = time.mktime(pd.to_datetime("2021-06-01 21:45:00").timetuple())
        for index, row in self.time_series.iterrows():
            if (row.command != last_val) | ((row.command != 0) & (row["name"] != last_name)):
                if not cur_active:
                    cur_active = True
                    start_time = row.unix
                    command = row.command
                else:
                    cur_active = False
                    stop_time = last_time
                    timeline.append({"start": start_time, "finish":stop_time, "value":command, "name":last_name})
            last_val = row.command
            last_name = row["name"]
            last_time = row.unix
        return timeline


    def run(self):
        self._app.run(host="0.0.0.0", port=8890, threaded=True)

    def up(self):
        return "OK"

    def return_tick(self):
        if self.counter == self.max_counter:
            return jsonify({"No more data"})
        data = self.time_series[self.time_series["time"] == self.time_series.iloc[self.counter]["time"]]
        # data = self.time_series.iloc[self.counter].to_json()
        self.counter += 1
        return data.to_json(orient="records")

    def reset_counter(self):
        self.counter = 0
        return "Done"

    def get_timeline(self):
        return jsonify(self.timeline)


if __name__=="__main__":

    api = BackendServer()
    api.run()

