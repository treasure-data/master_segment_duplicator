from PIL import Image, ImageDraw
import os

# Path to the input PNG file
input_file = "static/logo.png"
# Path to the output ICO file
output_file = "static/favicon.ico"

if not os.path.exists(input_file):
    print(f"Error: Input file '{input_file}' does not exist")
    exit(1)

try:
    # Open the PNG image
    img = Image.open(input_file)

    # Get image dimensions
    width, height = img.size

    # Convert to RGBA for transparency support
    img = img.convert("RGBA")

    # Create a simplified version - this will remove text by creating a simple icon
    # Create a blank square canvas with the same size as the shortest dimension
    size = min(width, height)
    icon_size = size

    # Create a new blank image with transparency
    simplified_img = Image.new("RGBA", (icon_size, icon_size), (0, 0, 0, 0))

    # Calculate the center region of the original image to extract
    left = (width - size) // 2
    top = (height - size) // 2
    right = left + size
    bottom = top + size

    # Crop the center part of the image (typically where the logo mark is)
    # This helps remove any text that might be around the edges
    center_img = img.crop((left, top, right, bottom))

    # Paste the center image onto our simplified canvas
    simplified_img.paste(center_img, (0, 0))

    # Save as ICO using the simplified image
    simplified_img.save(output_file, format="ICO", sizes=[(16, 16), (32, 32), (48, 48)])

    print(f"Successfully created {output_file}")
except Exception as e:
    print(f"Error converting image: {e}")
