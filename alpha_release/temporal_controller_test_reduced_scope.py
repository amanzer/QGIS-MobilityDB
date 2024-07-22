"""
Skelletton for the temporal controller unit

In this script we test the implementation of the Time Delta animation for MobilityDB layers within Move

"""

TIME_DELTA_SIZE = 30


def log(msg):
    QgsMessageLog.logMessage(msg, 'Move', level=Qgis.Info)


class Move:
    def __init__(self):
        # pymeos_initialize()
        self.iface= iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        
        # Attributes 
        self.frame = 0
        self.key = 0
        self.next_key = 0
        self.begin_frame = 0
        self.end_frame = 0

        self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
        self.total_frames = self.temporal_controller.totalFrameCount()
        self.temporal_controller.updateTemporalRange.connect(self.on_new_frame)

        # States for NavigationMode etc
        self.time_delta_size = TIME_DELTA_SIZE 
        # self.mobilitydb_layers= []
        self.execute()

    def execute(self):
        # connection_parameters = {
        #         'host': "localhost",
        #         'port': 5432,
        #         'dbname': DATABASE_NAME,
        #         'user': "postgres",
        #         'password': "postgres",
        #         'table_name': TPOINT_TABLE_NAME,
        #         'id_column_name': TPOINT_ID_COLUMN_NAME,
        #         'tpoint_column_name': TPOINT_COLUMN_NAME,
        #     }

        
        log("Creating animation for the following settings : \n")
        log(f"NavigationMode : {self.temporal_controller.navigationMode()}")
        log(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
        log(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
        log(f"Frame duration : {self.temporal_controller.frameDuration()}")
        log(f"isLooping : {self.temporal_controller.isLooping()}")
        log(f"FPS : {self.temporal_controller.framesPerSecond()}")
        log(f"Cumulative range : {self.temporal_controller.temporalRangeCumulative()}")

        log(f"\n Current Frame : {self.temporal_controller.currentFrameNumber()}")

        log("Executing")
        
        self.launch_animation()

        # log(f"Animation state : {self.temporal_controller.animationState()}")

        


    def launch_animation(self, key=0):
        self.key = key
        
        begin_frame = self.key*self.time_delta_size
        end_frame = (begin_frame + self.time_delta_size) - 1
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        self.begin_frame = begin_frame + self.time_delta_size
        self.end_frame = end_frame + self.time_delta_size

        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.begin_frame).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.end_frame).begin().toPyDateTime()
        
        self.next_key = self.key + 1
        # self.mobilitydb_layer_handler.start_animation(begin_ts_1, end_ts_1, key2, begin_ts_2, end_ts_2)

        log(f"Launching animation, first two time deltas \n First time delta : {begin_frame} to {end_frame} | {begin_ts_1} to {end_ts_1}  \n Second time delta: {self.begin_frame} to {self.end_frame} | {begin_ts_2} to {end_ts_2} ")



    def update_layers(self, current_frame):
        # self.mobilitydb_layer_handler.new_frame( self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())
        log(f"Update geometries for frame : {current_frame} with key {self.key}")


      

    def fetch_next_time_deltas(self, begin_frame, end_frame):
        begin_ts = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        # self.mobilitydb_layer_handler.fetch_time_delta(begin_ts, end_ts)
        log(f"Fetching Next Time Deltas : {begin_frame} to {end_frame} | {begin_ts} to {end_ts}")



    def on_new_frame(self):
        # Verify which signal is emitted
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()
        is_forward = (current_frame == next_frame)
        is_backward = (current_frame == previous_frame)

        log(f"$$New signal variables :\nCurrent Frame : {current_frame} | Next Frame : {next_frame} | Previous Frame : {previous_frame} | Forward : {is_forward} | Backward : {is_backward} ")

        if is_forward:
            log("Forward signal")
            self.frame = current_frame
            key = self.frame // self.time_delta_size

            if key == self.next_key and ((self.end_frame + self.time_delta_size) < self.total_frames): # Fetch Next time delta
                if self.task_manager.activeTaskCount() == 0:
                    log("Animation paused, waiting for next time delta to load")
                    self.temporal_controller.pause()
                self.key = key
                self.next_key = self.key + 1
                # self.switch_time_deltas()                        
                self.begin_frame = self.begin_frame + self.time_delta_size
                self.end_frame = self.end_frame + self.time_delta_size
                self.fetch_next_time_deltas(self.begin_frame, self.end_frame)
                log(f"next time delta : {self.begin_frame} to {self.end_frame} | self.key : {self.key} ")
                self.update_layers(self.frame)

        elif is_backward:
            log("Backward navigation only in the same time delta")
            key = current_frame // self.time_delta_size
            if key == self.key:
                self.frame = current_frame
                self.update_layers(self.frame)
            else:
                log("Different time delta")
                self.temporal_controller.pause()
                self.temporal_controller.setCurrentFrameNumber(self.frame)
        else:    
            """
            Multiple scenarios :
            -Navigation Mode change
            -Date Range change 
            -Time granularity change
            -FPS change
            -Cumulative FPS change
            - Verify if other scenario also trigger this signal

            """ 
            # self.frame = current_frame
            is_configuration_changed = False

            log("\n\n#### Signal is not for frame change ####\n\n")
            log(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
            log(f"temporalRangeCumulative : {self.temporal_controller.temporalRangeCumulative()}")
            log(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
            log(f"NavigationMode : {self.temporal_controller.navigationMode()}")
            log(f"isLooping : {self.temporal_controller.isLooping()}")
            log(f"FPS : {self.temporal_controller.framesPerSecond()}")
            log(f"Frame duration : {self.temporal_controller.frameDuration()}")
            log(f"Available temporal range : {self.temporal_controller.availableTemporalRanges()}")
            log(f"Animation state : {self.temporal_controller.animationState()}")
    
            log(f"Current Frame : {self.temporal_controller.currentFrameNumber()}")

            if self.temporal_controller.navigationMode() != self.navigationMode: # Navigation Mode change 
                is_configuration_changed = True
                if self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Animated:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                    log("Navigation Mode Animated")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Disabled:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Disabled
                    log("Navigation Mode Disabled")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Movie:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Movie
                    log("Navigation Mode Movie")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.FixedRange:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                    log("Navigation Mode FixedRange")

            elif self.temporal_controller.frameDuration() != self.frameDuration: # Frame duration change 
                is_configuration_changed = True
                log(f"Frame duration has changed from {self.frameDuration} with {self.total_frames} frames")
                self.frameDuration = self.temporal_controller.frameDuration()
                self.total_frames = self.temporal_controller.totalFrameCount()
                log(f"to {self.frameDuration} with {self.total_frames} frames")

            
            elif self.temporal_controller.temporalExtents() != self.temporalExtents:
                is_configuration_changed = True
                log(f"temporal extents have changed from {self.temporalExtents} with {self.total_frames} frames")
                self.temporalExtents = self.temporal_controller.temporalExtents()
                self.total_frames = self.temporal_controller.totalFrameCount()
                log(f"to {self.temporalExtents} with {self.total_frames} frames")   

            elif self.temporal_controller.temporalRangeCumulative() != self.cumulative_range:
                is_configuration_changed = True
                log(f"cumulative range has changed from {self.cumulative_range}")
                self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
                log(f"to {self.cumulative_range} with {self.total_frames} frames")
            
            if not is_configuration_changed:
                """
                Timeline has been skipped or other signal
                if the current frame is in the same time delta, we set the current frame back to self.frame, otherwise :
                1. Press load button again to reload the time deltas
                2. We set the current_frame back to self.frame

                """

                log("Timeline skipped or other signal")
                
                key = current_frame // self.time_delta_size
                if key == self.key:
                    log("Same time delta")
                    self.frame = current_frame
                    self.update_layers(self.frame)
                else:
                    log("Different time delta")
                    self.temporal_controller.pause()
                    self.temporal_controller.setCurrentFrameNumber(self.frame)
            else:
                log("Configuration changed, Press the load button to reload the animation")
                # self.reload_time_deltas()


    def reload_time_deltas(self):
        log("Changing internal configuration of temporal controller to match the new configuration \n")
        log("Restating the animation")

        self.frame = 0
        self.key = 0
        self.next_key = 0
        self.begin_frame = 0
        self.end_frame = 0

        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
        self.total_frames = self.temporal_controller.totalFrameCount()
        # self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        # self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.temporal_controller.setCurrentFrameNumber(0)
        # self.task_manager.cancelAll()
        self.launch_animation(self.key)
        
tt = Move()