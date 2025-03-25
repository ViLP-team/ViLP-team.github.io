import pandas as pd
from PIL import Image
import io
import base64

# Load the parquet file into a DataFrame
data = pd.read_parquet('ViLP.parquet')

def process_image(img_bytes):
    # Open the image from bytes
    with io.BytesIO(img_bytes) as img_buffer:
        with Image.open(img_buffer) as img:
            # Resize the image to 512x512
            img = img.resize((256, 256))
            # Save image as JPEG to an in-memory buffer with quality 85
            with io.BytesIO() as out_buffer:
                img.save(out_buffer, format="JPEG", quality=85)
                jpeg_bytes = out_buffer.getvalue()
    # Encode JPEG bytes to a base64 string
    encoded_str = base64.b64encode(jpeg_bytes).decode('utf-8')
    return "data:image/jpeg;base64," + encoded_str

# Identify columns that contain images (assumed to start with "image")
image_cols = [col for col in data.columns if col.startswith('image')]

# Process each image column in the DataFrame
for col in image_cols:
    data[col] = data[col].apply(process_image)

# Convert the entire DataFrame to a JSON string (records orientation)
json_str = data.to_json(orient="records")

# Optionally, write the JSON to a file
with open('vilp.json', 'w') as f:
    f.write(json_str)

# Strategies to reduce JSON size:
# - Converting images to JPEG (as shown) reduces raw image size.
# - Adjust the JPEG quality (lower value = smaller file, but lower quality).
# - Consider compressing the final JSON (e.g., using gzip) for storage or transmission.
