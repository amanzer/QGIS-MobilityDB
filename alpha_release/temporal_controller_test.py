"""
Skelletton for the temporal controller unit

In this script we test the implementation of the Time Delta animation for MobilityDB layers within Move

"""

TIME_DELTA_SIZE = 30

class Move:
    def __init__(self):
        # pymeos_initialize()
        self.iface= iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        self.frame = 0
        self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
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

        # self.mobilitydb_layer_handler = MobilitydbLayerHandler(self.iface, self.task_manager, SRID, connection_parameters)
        self.launch_animation()
        print(f"State of the temporal controller : ")
        print(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
        print(f"temporalRangeCumulative : {self.temporal_controller.temporalRangeCumulative()}")
        print(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
        print(f"NavigationMode : {self.temporal_controller.navigationMode()}")
        print(f"isLooping : {self.temporal_controller.isLooping()}")
        print(f"FPS : {self.temporal_controller.framesPerSecond()}")
        print(f"Frame duration : {self.temporal_controller.frameDuration()}")
        print(f"Available temporal range : {self.temporal_controller.availableTemporalRanges()}")
        print(f"Animation state : {self.temporal_controller.animationState()}")

        print(f"Current Frame : {self.temporal_controller.currentFrameNumber()}")

        print("Executing")


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
        
        key2 = self.key + 1
        self.direction = True # Forward
        # self.mobilitydb_layer_handler.start_animation(begin_ts_1, end_ts_1, key2, begin_ts_2, end_ts_2)

        print(f" Launch animation \n First time delta : {begin_frame} to {end_frame} | {begin_ts_1} to {end_ts_1}  \n Second time delta: {self.begin_frame} to {self.end_frame} | {begin_ts_2} to {end_ts_2} ")



    def update_layers(self, current_frame):
        # self.mobilitydb_layer_handler.new_frame( self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())
        print(f"Update geometries for frame : {current_frame} with key {self.key}")

    def switch_time_deltas(self):
        print(f"switch time deltas, current key : {self.key}")
      

    def fetch_next_time_deltas(self, begin_frame, end_frame):
        begin_ts = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        # self.mobilitydb_layer_handler.fetch_time_delta(begin_ts, end_ts)
        print(f"Fetching Next Time Deltas : {begin_frame} to {end_frame} | {begin_ts} to {end_ts}")



    def on_new_frame(self):
        print(f"$$start")
        # Verify which signal is emitted
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()
        is_forward = (current_frame == next_frame)
        is_backward = (current_frame == previous_frame)

        print(f"Call variables :\nCurrent Frame : {current_frame} | Next Frame : {next_frame} | Previous Frame : {previous_frame} | Forward : {is_forward} | Backward : {is_backward} | direction {self.direction}")

        if (is_forward or is_backward ) and (self.navigationMode == QgsTemporalNavigationObject.NavigationMode.Animated):
            print("!!! Signal came from Frame change")
            self.frame = current_frame
            
            key = self.frame // self.time_delta_size
            
            if is_forward:
                if self.direction: # Stays in the same direction
                    if key == self.key + 1 and ((self.end_frame + self.time_delta_size) < self.total_frames): # Fetch Next time delta
                        self.key = key
                        self.switch_time_deltas()                        
                        self.begin_frame = self.begin_frame + self.time_delta_size
                        self.end_frame = self.end_frame + self.time_delta_size
                        self.fetch_next_time_deltas(self.begin_frame, self.end_frame)
                        print(f"forward -> same direction -> next time delta : {self.begin_frame} to {self.end_frame} | direction {self.direction} |self.key : {self.key} ")

                    # elif key != self.key: # User has skipped through the time line, reload 2 time deltas 
                    #     self.launch_animation(key)
                    #     print(f"forward -> same direction -> skipped timeline : {self.begin_frame} to {self.end_frame} | direction {self.direction} |self.key : {self.key} ")
                    
                    self.update_layers(self.frame)
                        
                else: # Direction changed
                    # TODO : Reset the Buffer of time deltas
                    self.direction = True # Reverse
                    print(f"forward -> direction has changed ! : {self.begin_frame} to {self.end_frame} | direction {self.direction}")
                    pass

            elif is_backward:
                if not self.direction: # Stays in the same direction
                    if key == self.key - 1 and (self.begin_frame - self.time_delta_size >= 0): # Fetch Next time delta
                        self.key = key
                        self.switch_time_deltas()
                        self.begin_frame = self.begin_frame - self.time_delta_size
                        self.end_frame = self.end_frame - self.time_delta_size
                        self.fetch_next_time_deltas(self.begin_frame, self.end_frame)
                        print(f"backward -> same direction -> next time delta : {self.begin_frame} to {self.end_frame} | direction {self.direction} |self.key : {self.key} ")

                    # elif key != self.key: # User has skipped through the time line, reload 2 time deltas
                    #     self.launch_animation(key) # TODO: Currently we assume it is a forward direction
                    #     print(f"backward -> same direction -> skipped timeline : {self.begin_frame} to {self.end_frame} | direction {self.direction} |self.key : {self.key} ")
                    self.update_layers(self.frame)
                else: # Direction changed
                    # TODO : Reset the Buffer of time deltas
                    self.direction = False # Forward
                    print(f"backward -> direction has changed ! : {self.begin_frame} to {self.end_frame}  | direction {self.direction} ")
                    pass

            
        else:
            self.frame = current_frame
            """
            Multiple scenarios :
            -Navigation Mode change
            -Date Range change 
            -Time granularity change
            -FPS change
            -Cumulative FPS change
            - Verify if other scenario also trigger this signal

            """ 
            print("\n\n#### Signal is not for frame change ####\n\n")
            print(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
            print(f"temporalRangeCumulative : {self.temporal_controller.temporalRangeCumulative()}")
            print(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
            print(f"NavigationMode : {self.temporal_controller.navigationMode()}")
            print(f"isLooping : {self.temporal_controller.isLooping()}")
            print(f"FPS : {self.temporal_controller.framesPerSecond()}")
            print(f"Frame duration : {self.temporal_controller.frameDuration()}")
            print(f"Available temporal range : {self.temporal_controller.availableTemporalRanges()}")
            print(f"Animation state : {self.temporal_controller.animationState()}")
    
            print(f"Current Frame : {self.temporal_controller.currentFrameNumber()}")

            if self.temporal_controller.navigationMode() != self.navigationMode: # Navigation Mode change -> For now only allow animated mode
                if self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Animated:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                    print("Navigation Mode Animated")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Disabled:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Disabled
                    print("Navigation Mode Disabled")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Movie:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Movie
                    print("Navigation Mode Movie")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.FixedRange:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                    print("Navigation Mode FixedRange")


            elif self.temporal_controller.frameDuration() != self.frameDuration: # Frame duration change ==> Restart animation
                print(f"Frame duration has changed from {self.frameDuration} with {self.total_frames} frames")
                self.frameDuration = self.temporal_controller.frameDuration()
                self.total_frames = self.temporal_controller.totalFrameCount()
                print(f"to {self.frameDuration} with {self.total_frames} frames")

                print(f"Delete all time deltas from layers, reload 2 time deltas for the current key ") # TODO : even load 3 time deltas ? to allow both directions for the user.
                self.reload_time_deltas()
            
            elif self.temporal_controller.temporalExtents() != self.temporalExtents:
                print(f"temporal extents have changed from {self.temporalExtents} with {self.total_frames} frames")
                self.temporalExtents = self.temporal_controller.temporalExtents()
                self.total_frames = self.temporal_controller.totalFrameCount()
                print(f"to {self.temporalExtents} with {self.total_frames} frames")   
            else:
                # Not handled : FPS change/cumulative range(no signal sent), animation state => Not handled, loop state => Not handled
                
                # Currently we assume this just means the user has skipped through the timeline
                key = current_frame // self.time_delta_size
                if key != self.key:
                    self.reload_time_deltas()
                    print(f"Reload time deltas for the new key {key}")
                else:
                    print("timeline skipped in the same key")


    def reload_time_deltas(self):
        self.frame = self.temporal_controller.currentFrameNumber()
        self.key = self.frame // self.time_delta_size
        # self.movelayer_handler.delete_all_time_deltas()
        print("Reload Time deltas")
        self.launch_animation(self.key)
        
tt = Move()