from PIL import Image, ImageDraw
import json

with open('./rose_static/colors.json') as f:
    colors = json.load(f)

with open('./rose_static/coordinates.json') as f:
    coordinates = json.load(f)

def hex_to_rgb(hex_code):
    """Converts a hex color code to an RGB tuple."""
    hex_code = hex_code.lstrip('#')
    # the tuple is in the order (R, G, B, A)
    # A can be modified to change the transparency of the color (0 is transparent, 255 is opaque)
    # (lines will still be fully opaque)
    return tuple(int(hex_code[i:i+2], 16) for i in (0, 2, 4))  + (255,)

def fill_region(img, coordinates, fill_color_hex):
    # convert hexcode to RGBA
    fill_color = hex_to_rgb(fill_color_hex)
    fill_img = img.copy()
    ImageDraw.floodfill(fill_img, coordinates, fill_color)
    return fill_img

def num_to_danger(num):
    if num == 0:
        return 'none'
    elif num == 1:
        return 'low'
    elif num == 2:
        return 'moderate'
    elif num == 3:
        return 'considerable'
    elif num == 4:
        return 'high'
    elif num == 5:
        return 'extreme'

def create_rose(image_path, input):
    img = Image.open(image_path).convert('RGBA')
    
    for level, directions in input.items():
        for direction, danger in directions.items():
            for color in ['colors', 'light-shadow', 'dark-shadow']:
                try:
                    for coord in coordinates["-".join([level,color])][direction]:
                        if danger.isnumeric():
                            danger = num_to_danger(int(danger))
                        img = fill_region(img, coord, colors[color][danger])
                except KeyError:
                    pass
    
    return img

if __name__ == '__main__':
    # Test input data
    input = {
        "bottom": {
            "N": "none",
            "NE": "low",
            "E": "low",
            "SE": "low",
            "S": "low",
            "SW": "low",
            "SW": "moderate",
            'W': 'considerable',
            'NW': 'considerable'},
        'middle': 
            {'N': 'low',
            'NE': 'moderate',
            'E': 'moderate',
            'SE': 'considerable',
            'S': 'low',
            'SW': 'high',
            'W': 'low',
            'NW': 'high'},
        'top': 
            {'N': 'extreme',
            'NE': 'extreme',
            'E': 'moderate',
            'SE': 'moderate',
            'S': 'high',
            'SW': 'extreme',
            'W': 'extreme',
            'NW': 'extreme'
        }
    }

    input_num = {
        "bottom": {
            "N": "3",
            "NE": "3",
            "E": "3",
            "SE": "3",
            "S": "2",
            "SW": "2",
            "W": "3",
            "NW": "3"
        },
        "middle": {
            "N": "4",
            "NE": "3",
            "E": "3",
            "SE": "3",
            "S": "3",
            "SW": "4",
            "W": "4",
            "NW": "4"
        },
        "top": {
            "N": "5",
            "NE": "5",
            "E": "4",
            "SE": "2",
            "S": "4",
            "SW": "4",
            "W": "5",
            "NW": "5"
        }
    }

    # Test usage
    image_path = "./rose_static/blank-rose.png"
    filled_image = create_rose(image_path, input)
    filled_image.save("./generated-roses/example.png")
