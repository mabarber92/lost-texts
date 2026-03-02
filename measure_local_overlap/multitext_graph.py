import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection
from collections import Counter
import math
from tqdm import tqdm
import numpy as np

class multitextGraph():

    def __init__ (self, mapping_json, metadata_csv=None):
        self.mapping_dict = self.load_json(mapping_json)


        # metadata_csv allows to provide transliterations or translations of the data and URI mappings
        # if given, only the uris in the meta are used for the analysis
        if metadata_csv is not None:
            self.meta_df = pd.read_csv(metadata_csv)
            # Filter the data
            # Map the metadata into the json
        
        


        # Get the highest number of characters and number of books
        self._get_summary_data()

    def load_json(self, json_path):

        with open(json_path, encoding='utf-8') as f:
            data = json.load(f)
          
        return data
    
    def filter_uris(self, uri_list):
        new_dict = {}
        for uri in uri_list:
            new_dict[uri] = self.mapping_dict[uri]
        print(f"Data filtered from {len(self.mapping_dict)} to {len(new_dict)} uris")
        self.mapping_dict = new_dict

    # Function to get max chars
    def _get_summary_data(self):
        max_chars = 0
        max_intensity = 0
        for book, data in self.mapping_dict.items():
            book_max = 0
            book_intensity_max = 0
            for section, section_data in data.items():                
                res = Counter()
                for patches in section_data["patches"]:
                    for k, v in patches.items():
                        res[k] = max(res[k], v)
                book_max += res["end"]
                book_intensity_max += res["intensity"]
            if book_max > max_chars:
                max_chars = book_max
            if book_intensity_max < max_intensity:
                max_intesity = book_intensity_max

        self.max_chars = max_chars
        self.max_intensity = max_intensity
        self.book_count = len(self.mapping_dict.keys()) 

    def _calculate_line_length(self, max_lines):
        line_length = math.ceil(self.max_chars/max_lines)
        return line_length

    def _create_patches_df(self, book_section_patches):
        """For book sections dictionary - take the patches and create a df - augment the char count
        Add rows for the section annotations"""
        char_pos = 0
        df_out = pd.DataFrame()
        for section, data in book_section_patches.items():
            
            # Round up the char_pos to the nearest section start (to avoid starting in the middle of a row)
            if char_pos != 0:
                nearest_line = math.ceil(char_pos/self.line_length)
                char_pos = (nearest_line + 1) * self.line_length

            
            # Prepare the annotation row
            annotation = [{"start": char_pos,
                           "end": char_pos+self.line_length,
                           "intensity": 0,
                           "type": "annotation",
                           "label": section}]
            annotation_df = pd.DataFrame(annotation)
            
            # Augment the char pos in the patches to account for current char pos + annotation row
            char_pos += self.line_length
            patches_df = pd.DataFrame(data["patches"])
            patches_df["type"] = "reuse"
            patches_df["label"] = None

            patches_df["start"] += char_pos
            patches_df["end"] += char_pos

            df_out = pd.concat([df_out, annotation_df, patches_df])

        return df_out

    def _add_patch_data(self, start, width, current_height, height_increase, patch_list, color_list,  intensity = None, color=None):
            """Function to update patch lists"""
            
            patch = self._create_rectangle(start, width, current_height, height_increase)
            patch_list.append(patch)
            # Write intensity or color
            if intensity is not None:
                color_list.append(intensity)
            elif color is not None:
                color_list.append(color)
            
            return patch_list, color_list

    # GPT solution - works better, but still whitespace issue
    # def _write_data_to_patch(self, start_offset, end_offset, current_wrap, current_height,
    #                         height_increase, column_pos, intensity=None, color=None):

    #     if color is None and intensity is None:
    #         raise ValueError("Color or intensity must be passed to _write_data_to_patch()")

    #     patches, colors = [], []
    #     width = end_offset - start_offset

    #     if self.line_length <= 0:
    #         raise ValueError(f"line_length must be > 0, got {self.line_length}")
    #     if width < 0:
    #         raise ValueError(f"end_offset < start_offset: start={start_offset}, end={end_offset}, width={width}")

    #     # We'll move through the interval left-to-right
    #     remaining = width
    #     offset_cursor = start_offset

    #     while remaining > 0:
    #         # position within the current line (0..line_length-1)
    #         pos_in_line = (offset_cursor + current_wrap) % self.line_length

    #         # actual x coordinate includes the column offset
    #         x = column_pos + pos_in_line

    #         # how much space left on this line from pos_in_line
    #         space = self.line_length - pos_in_line

    #         patch_len = min(remaining, space)  # always > 0 if remaining > 0

    #         patches, colors = self._add_patch_data(
    #             x, patch_len, current_height, height_increase,
    #             patches, colors, intensity, color
    #         )

    #         remaining -= patch_len
    #         offset_cursor += patch_len

    #         # if we exactly filled to line end, go to next row
    #         if patch_len == space:
    #             current_height += height_increase

    #     new_wrap = (start_offset + width + current_wrap) % self.line_length
    #     return patches, colors, new_wrap, current_height

    def _write_data_to_patch(self, start_offset, end_offset, current_wrap, current_height, height_increase, column_pos, intensity=None, color=None):
        """Take a data point and write as many patches as needed to deal with the length of the given data point
        To use for empty lines - give start_offset as 0 and end_offset as line length
        Returns: list of written patches, list of related colors or intensities, depending on which is given
        new_height, new_wrap"""
        patches = []
        colors = []
        # Offsets still not working - creating a staggered result.
        # Set the start pos using the current wrap + column_pos (which column are we in) - with all additional cols we're getting stuck in infinite loop
        start_pos = (start_offset % self.line_length) + current_wrap + column_pos
        # print(f"Start pos: {start_pos}")
        # print(f"Line max: {self.line_length + column_pos}")
        width = end_offset - start_offset
        # Write patches until the width is less than line length
        
        
        while width > self.line_length:
            patch_len = self.line_length + column_pos - start_pos # This fix is the issue
            patches, colors = self._add_patch_data(start_pos, patch_len, current_height, height_increase, patches, colors, intensity, color)
            
            
            # Update width by removing patch_len (giving us remainder) - if width is longer than patch_len
            if width > patch_len:
                width -= patch_len
            
            # Update the start_pos to start of column - as we'll carry the wrap at the very end
            start_pos = column_pos
            # Augment current height
            current_height += height_increase
        
        # Once the width is down to under the line length, write out the final patch with remaining width
        patches, colors = self._add_patch_data(start_pos, width, current_height, height_increase, patches, colors, intensity, color)
        # Increase the height ready for next pass
        current_height += height_increase
        # The new wrap is the outstanding width
        new_wrap = width

        return patches, colors, new_wrap, current_height






    def _write_patches(self, max_lines, nonreuse_color = "grey"):
        """Create a list of rectangular patches using the line length and the data dict"""
        # Will need to return the locations for labels alongside the gaps (or even just an annotation-compliant format that we can pass to matplotlib)
        print("Writing patches...")
        height_increase = -1 # A bit basic - multiplying the increase by the book count
        patches_list = []
        patch_intensity = []
        gap_patches_list = []
        gap_colors = []
        
        annotations_list = []
  
        horizontal_markers = []

        #
        
        # Set horizontal position at zero - will increase by char len + 1 with each book
        horizontal_pos = 0
        # Loop through each book
        for book, data in tqdm(self.mapping_dict.items()):
            
            # Reset height
            height = 0
            # Set initial wrap (to handle wrapping)
            wrap = 0
            # For each book add new lines for each section - this should only be calculated on max chars in the book (or it'll fill out beyond length of section - this fix still needed)
            book_lines = max_lines + len(data.keys())
            
            # Convert the data into a df for easy filtering - but need to handle having gap or annotation for the diff sections
            data_df = self._create_patches_df(data)

            # Add book title to annotation?
            
            # Loop up to max_chars with steps of line_length
            for i in range(0, book_lines*self.line_length, self.line_length):
                # Calculate the line end - deduct 1 to avoid overlap and double counting
                line_end = i + self.line_length - 1
                # Filter the df for the relevant sections
                line_data = data_df[data_df["start"].between(i, line_end)]
                # If line_data is empty, then add a noreuse_color to the empty line and reset the wrap to zero
                if len(line_data) == 0:
                    
                    if nonreuse_color is not None:
                        new_gap_patches, new_gap_colors, wrap, height = self._write_data_to_patch(0, self.line_length, wrap, height, height_increase, horizontal_pos, color=nonreuse_color)
                        gap_patches_list.extend(new_gap_patches)
                        gap_colors.extend(new_gap_colors)
                    
        

                else:
                # If only annotation in the row skip and just reset overlap and height - as check, throw error if annotation and data in the row
                
                    types = line_data["type"].drop_duplicates().tolist()
                    if "annotation" in types and "reuse" not in types:
                        height += height_increase
                        wrap = 0
                        # Add annotation to our annotations list
                    # This is an error check - if logic of _create_patches_df correct, then this shouldn't trip
                    elif "annotation" in types and "reuse" in types:
                        print("Offset alignment is misaligned - mixed annotation and data in a row")
                        print(f"Current offset: {i}")
                        print("Filtered dataframe:")
                        print(line_data)
                    
                    else:
                        # Else - loop through the data and produce the patch rectangles
                        listed_data = line_data.sort_values(by=["start"]).to_dict("records")
                        row_count = len(listed_data)
                        for idx, row in enumerate(listed_data):
                            
                            # Write the data to a patch - the function will handle lines longer than line length and wrap
                            
                            new_patches, new_intensities, wrap, height = self._write_data_to_patch(row["start"], row["end"], wrap, height, height_increase, horizontal_pos, intensity = row["intensity"])
                            patches_list.extend(new_patches)
                            patch_intensity.extend(new_intensities)
                            # If nonreuse_color is not None - check if there is a gap between this offset and the next one - if there is create a gap patch
                            if nonreuse_color is not None and idx < row_count-1:
                                next_start = listed_data[idx+1]["start"]
                                if next_start - row["start"] > 0:
                                    new_gap_patches, new_gap_colors, wrap, height = self._write_data_to_patch(row["end"], next_start, wrap, height, height_increase, horizontal_pos, color=nonreuse_color)
                                    gap_patches_list.extend(new_gap_patches)
                                    gap_colors.extend(new_gap_colors)

            # Update horizontal for next book - use + 1 to create a gap
            
            horizontal_pos += self.line_length + 1
            horizontal_markers.append(horizontal_pos)
                    

                # Handle each section

                    # Get only items under the line length (will need to handle wrapping - cut ranges that cross the edge of the line)
                    # Write rectangles to the patches_list
                    # Handle gaps with the colour set for nonreuse_color
                    # Add to the height (e.g. -1 - will need to augment this based on book count (as that determines width of graph))
            # Add to width (ready for next book)
        
        
        # Create a collection from the patches
        
        patch_collection = PatchCollection(patches_list, cmap=self.cmap, norm=self.norm, edgecolor='none')
        patch_collection.set_array(np.asarray(patch_intensity))

        # Create a separate patch collection for the gaps
        gap_patch_collection = PatchCollection(gap_patches_list, facecolors=gap_colors)

        # Return the patch collections and the annotation
        return patch_collection, gap_patch_collection, annotations_list, horizontal_markers
    
    def _set_horizontal_position(self, data_offset, absolute_pos, horizontal_pos, wrap):
        """Calculate the position of an offset based on the current position in the 
        data (absolute_pos) and the horizontal_pos of the graph and adjustment for wrapping"""
        
        position = horizontal_pos + wrap + absolute_pos + self.line_length - data_offset
        
        return position

    def _create_rectangle(self, start, width, current_height, height_increase, data=None):
        """Use data about start, end position and height to create a rectangle using that data"""
        
        xy = (start, current_height)
        # width = end-start
        # if width > self.line_length or width < 0:
        #     print(width)
            
        rect = Rectangle(xy, width, height_increase)
        return rect


    def _set_color_mapping(self, color_map="YlOrBr"):
        """ Initiate a sequential color heatmap - using max intensity as top of scale"""
        self.cmap = plt.get_cmap(color_map)
        self.norm = mcolors.Normalize(vmin=0, vmax=self.max_intensity)

    def _fetch_color_mapping(self, intensity):
        """Function to allow less verbose code for passing the color to the patches"""
        return self.cmap(self.norm(intensity))  

    # Overall graphing function
    def draw_diff_graph(self, max_lines=500, color_map = "YlOrBr", export_path=None):
        """Main func for drawing the graph - max lines is the number of lines to go to for the longest book"""
        self.line_length = self._calculate_line_length(max_lines)
        self._set_color_mapping(color_map)
        patch_collection, gap_patch_collection, annotation_list, horizontal_markers = self._write_patches(max_lines=max_lines)
        print(horizontal_markers)
        print(patch_collection)
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        
        ax.add_collection(patch_collection)
        ax.add_collection(gap_patch_collection)
        ax.vlines(horizontal_markers, ymin=0, ymax=0-max_lines)

        ax.set_xlim(0, (self.line_length+1) * self.book_count)     # or your max end offset
        
        ax.set_ylim(0, 0 - max_lines) 

        # Add a colorbar
        plt.colorbar(patch_collection, ax=ax)

        if export_path is not None:
            fig.savefig(export_path, bbox_inches="tight", dpi=300)
        else:
            plt.show()
