"""Convert coco captions from json to csv format
that can easily be loaded by apache beam."""

from __future__ import annotations
import json

from tqdm import tqdm


def coco_json_to_csv(filename: str, limit: int = None):
    with open(filename, 'r') as f:
        coco_dict = json.load(f)

    images = coco_dict['images']
    annotations = coco_dict['annotations']
    licenses = coco_dict['licenses']
    license_name_by_id = {}
    for license in licenses:
        license_name_by_id[license['id']] = license['name']

    limit = limit if limit is not None else len(images)
    annotation_dicts = []
    for annotation in tqdm(annotations[:limit]):
        for image in images:
            if annotation['image_id'] == image['id']:
                annotation['image_url'] = image['url']
                annotation['image_name'] == image['file_name']
                annotation['image_license'] = license_name_by_id[image['license']]
        annotation_dicts.append(annotation)

    with open(filename.replace('.json', '.jsonl'), 'w') as output_file:
        output_file.writelines([json.dumps(annotation_dict) + "\n"
                                for annotation_dict in annotation_dicts])


if __name__ == "__main__":
    coco_json_to_csv("./captions_val2014.json", limit=500)