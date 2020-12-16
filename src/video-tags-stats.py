import os
from collections import defaultdict
import pandas as pd
import json

import supervisely_lib as sly
from supervisely_lib.video_annotation.key_id_map import KeyIdMap

my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])
DATASET_ID = os.environ.get('modal.state.slyDatasetId', None)
if DATASET_ID is not None:
    DATASET_ID = int(DATASET_ID)

PROJECT = None
TOTAL = 'total'


def process_video_annotation(ann, property_tags_counter, info_counter, ds_name, video_info):
    for tag in ann.tags:
        if tag.frame_range is None:
            info_counter[tag.name][TOTAL].extend([video_info])
            info_counter[tag.name][ds_name].extend([video_info])
            property_tags_counter[tag.name] += 1


def process_video_annotation_tags_values(ann, property_tags_values_counter, val_info_counter, ds_name, video_info):
    for tag in ann.tags:
        if tag.frame_range is None:
            val_info_counter[tag.name][tag.value][TOTAL].extend([video_info])
            val_info_counter[tag.name][tag.value][ds_name].extend([video_info])
            property_tags_values_counter[tag.name][tag.value] += 1


def process_video_ann_frame_tags(ann, frame_range_tags_counter, frame_info_counter, ds_name, video_info, tags_counter):
    for tag in ann.tags:
        if tag.frame_range:
            frame_info_counter[tag.name][TOTAL].extend([video_info])
            frame_info_counter[tag.name][ds_name].extend([video_info])
            number_of_frames = tag.frame_range[1] - tag.frame_range[0] + 1
            frame_range_tags_counter[tag.name] += number_of_frames
            tags_counter[tag.name] += 1


def process_video_ann_frame_tags_vals(ann, frame_range_tags_val_counter, val_frame_info_counter, ds_name, video_info, tags_values_counter):
    for tag in ann.tags:
        if tag.frame_range:
            val_frame_info_counter[tag.name][tag.value][TOTAL].extend([video_info])
            val_frame_info_counter[tag.name][tag.value][ds_name].extend([video_info])
            number_of_frames = tag.frame_range[1] - tag.frame_range[0] + 1
            frame_range_tags_val_counter[tag.name][tag.value] += number_of_frames
            tags_values_counter[tag.name][tag.value] += 1


@my_app.callback("calculate_stats")
@sly.timeit
def calculate_stats(api: sly.Api, task_id, context, state, app_logger):
    total_count = PROJECT.items_count
    if DATASET_ID is not None:
        total_count = api.dataset.get_info_by_id(DATASET_ID).items_count
    progress = sly.Progress("Processing video labels ...", total_count, app_logger)

    fields = [
        {"field": "data.started", "payload": True},
        {"field": "data.progressCurrent", "payload": 0},
        {"field": "data.progressTotal", "payload": total_count},
    ]
    api.app.set_fields(task_id, fields)

    meta_json = api.project.get_meta(PROJECT.id)
    meta = sly.ProjectMeta.from_json(meta_json)
    if len(meta.obj_classes) == 0:
        raise ValueError("There are no object classes in project")

    columns = ['#', 'tag']
    columns_frame_tag = ['#', 'tag'] #===========frame_tags=======
    columns_for_values = ['#', 'tag', 'tag_value']
    columns_frame_tag_values = ['#', 'tag', 'tag_value'] #===========frame_tags=======
    if DATASET_ID is None:
        columns.extend(['total'])
        columns_for_values.extend(['total'])
        columns_frame_tag.extend(['total', 'total_cnt']) #===========frame_tags=======
        columns_frame_tag_values.extend(['total', 'total_cnt']) #===========frame_tags=======

    datasets_counts = []
    datasets_values_counts = []
    datasets_frame_tag_counts = [] #===========frame_tags=======
    datasets_frame_tag_values_counts = [] #===========frame_tags=======

    key_id_map = KeyIdMap()

    video_info_counter = defaultdict(lambda: defaultdict(list))
    video_info_counter_values = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    video_frames_info_counter = defaultdict(lambda: defaultdict(list))
    video_frames_info_counter_values = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for dataset in api.dataset.get_list(PROJECT.id):
        if DATASET_ID is not None and dataset.id != DATASET_ID:
            continue

        columns.extend([dataset.name])
        ds_property_tags = defaultdict(int)

        columns_for_values.extend([dataset.name])
        ds_property_tags_values = defaultdict(lambda: defaultdict(int))
        #===========frame_tags=========================================
        columns_frame_tag.extend([dataset.name, dataset.name + '_cnt'])
        ds_frame_tags = defaultdict(int)
        ds_frame_tags_counter = defaultdict(int)

        columns_frame_tag_values.extend([dataset.name, dataset.name + '_cnt'])
        ds_frame_tags_values = defaultdict(lambda: defaultdict(int))
        ds_frame_tags_values_counter = defaultdict(lambda: defaultdict(int))
        # ===========frame_tags=========================================

        videos = api.video.get_list(dataset.id)
        for video_info in videos:
            ann_info = api.video.annotation.download(video_info.id)
            ann = sly.VideoAnnotation.from_json(ann_info, meta, key_id_map)
            process_video_annotation(ann, ds_property_tags, video_info_counter, dataset.name, video_info)
            process_video_annotation_tags_values(ann, ds_property_tags_values, video_info_counter_values, dataset.name, video_info)

            process_video_ann_frame_tags(ann, ds_frame_tags, video_frames_info_counter, dataset.name, video_info, ds_frame_tags_counter) #===========frame_tags=======
            process_video_ann_frame_tags_vals(ann, ds_frame_tags_values, video_frames_info_counter_values, dataset.name, video_info, ds_frame_tags_values_counter) #===========frame_tags=======
            progress.iter_done_report()
            api.app.set_fields(task_id, fields=[
                {"field": "data.progressCurrent", "payload": progress.current},
                {"field": "data.progress", "payload": int(progress.current * 100 / total_count)},
            ])
        datasets_counts.append((dataset.name, ds_property_tags))
        datasets_values_counts.append((dataset.name, ds_property_tags_values))
        datasets_frame_tag_counts.append((dataset.name, ds_frame_tags)) #===========frame_tags=======
        datasets_frame_tag_values_counts.append((dataset.name, ds_frame_tags_values)) #===========frame_tags=======

    data = []
    for idx, tag_meta in enumerate(meta.tag_metas):
        name = tag_meta.name
        row = [idx, name]
        if DATASET_ID is None:
            row.extend([0])
        for ds_name, ds_property_tags in datasets_counts:
            row.extend([ds_property_tags[name]])
            if DATASET_ID is None:
                row[2] += ds_property_tags[name]
        data.append(row)

    df = pd.DataFrame(data, columns=columns)
    total_row = list(df.sum(axis=0))
    total_row[0] = len(df)
    total_row[1] = 'Total'
    df.loc[len(df)] = total_row
    print(df)
    print(video_info_counter)
    print('=' * 100)
    #=========property_tags_values=========================================================
    data_values = []
    idx = 0
    for ds_property_tags_values in datasets_values_counts:
        for tag_name, tag_vals in ds_property_tags_values[1].items():
            for val, cnt in tag_vals.items():
                row_val = [idx, tag_name, str(val)]
                if DATASET_ID is None:
                    row_val.extend([0])
                    row_val.extend([cnt])
                    row_val[3] += cnt
                data_values.append(row_val)
                idx += 1
    df_values = pd.DataFrame(data_values, columns=columns_for_values)
    total_row = list(df_values.sum(axis=0))
    total_row[0] = len(df_values)
    total_row[1] = 'Total'
    total_row[2] = 'Total'
    df_values.loc[len(df_values)] = total_row
    print(df_values)
    print(video_info_counter_values)
    print('=' * 100)
    # =======================================================================================

    # =========frame_tag=====================================================================
    data_frame_tags = []
    for idx, tag_meta in enumerate(meta.tag_metas):
        name = tag_meta.name
        row_frame_tags = [idx, name]
        if DATASET_ID is None:
            row_frame_tags.extend([0, 0])
        for ds_name, ds_frame_tags in datasets_frame_tag_counts:
            row_frame_tags.extend([ds_frame_tags[name], ds_frame_tags_counter[name]])
            if DATASET_ID is None:
                row_frame_tags[2] += ds_frame_tags[name]
                row_frame_tags[3] += ds_frame_tags_counter[name]
        data_frame_tags.append(row_frame_tags)

    df_frame_tags = pd.DataFrame(data_frame_tags, columns=columns_frame_tag)
    total_row = list(df_frame_tags.sum(axis=0))
    total_row[0] = len(df_frame_tags)
    total_row[1] = 'Total'
    df_frame_tags.loc[len(df_frame_tags)] = total_row
    print(df_frame_tags)
    print(video_frames_info_counter)
    print('=' * 100)
    # =======================================================================================

    # =========frame_tags_values=============================================================
    data_frame_tags_values = []
    idx = 0
    for ds_frame_tags_values in datasets_frame_tag_values_counts:
        for tag_name, tag_vals in ds_frame_tags_values[1].items():
            for val, cnt in tag_vals.items():
                row_frame_tags_val = [idx, tag_name, str(val)]
                if DATASET_ID is None:
                    row_frame_tags_val.extend([0, 0])
                    row_frame_tags_val.extend([cnt])
                    row_frame_tags_val[3] += cnt
                    row_frame_tags_val[4] += ds_frame_tags_values_counter[tag_name][val]
                    row_frame_tags_val.extend([ds_frame_tags_values_counter[tag_name][val]])
                data_frame_tags_values.append(row_frame_tags_val)
                idx += 1
    df_frame_tags_values = pd.DataFrame(data_frame_tags_values, columns=columns_frame_tag_values)
    total_row = list(df_frame_tags_values.sum(axis=0))
    total_row[0] = len(df_frame_tags_values)
    total_row[1] = 'Total'
    total_row[2] = 'Total'
    df_frame_tags_values.loc[len(df_frame_tags_values)] = total_row
    print(df_frame_tags_values)
    print(video_frames_info_counter_values)
    # =======================================================================================

    # df = df.append(total_row, ignore_index=True)

    # save report to file *.lnk (link to report)
    '''
    report_name = "{}_{}.lnk".format(PROJECT.id, PROJECT.name)
    local_path = os.path.join(my_app.data_dir, report_name)
    sly.fs.ensure_base_path(local_path)
    with open(local_path, "w") as text_file:
        print(my_app.app_url, file=text_file)
    remote_path = "/reports/video_objects_stats_for_every_class/{}".format(report_name)
    remote_path = api.file.get_free_name(TEAM_ID, remote_path)
    report_name = sly.fs.get_file_name_with_ext(remote_path)
    file_info = api.file.upload(TEAM_ID, local_path, remote_path)
    report_url = api.file.get_url(file_info.id)

    fields = [
        {"field": "data.loading", "payload": False},
        {"field": "data.table", "payload": json.loads(df.to_json(orient="split"))},
        {"field": "data.savePath", "payload": remote_path},
        {"field": "data.reportName", "payload": report_name},
        {"field": "data.reportUrl", "payload": report_url},
    ]
    api.app.set_fields(task_id, fields)
    api.task.set_output_report(task_id, file_info.id, report_name)
    '''
    my_app.stop()


def main():
    global PROJECT
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "VIDEO_PROJECT_ID": PROJECT_ID,
        "VIDEO_DATASET_ID": DATASET_ID
    })

    api = my_app.public_api
    PROJECT = api.project.get_info_by_id(PROJECT_ID)
    if PROJECT is None:
        raise RuntimeError("Project {!r} not found".format(PROJECT.name))
    if PROJECT.type != str(sly.ProjectType.VIDEOS):
        raise TypeError("Project type is {!r}, but has to be {!r}".format(PROJECT.type, sly.ProjectType.VIDEOS))

    data = {}
    state = {}

    # input card
    data["projectId"] = PROJECT.id
    data["projectName"] = PROJECT.name
    data["projectPreviewUrl"] = api.image.preview_url(PROJECT.reference_image_url, 100, 100)

    # output card
    data["progressCurrent"] = 0
    data["progressTotal"] = PROJECT.items_count
    data["progress"] = 0
    data["loading"] = True

    # sly-table
    data["table"] = {"columns": [], "data": []}

    # Run application service
    my_app.run(data=data, state=state, initial_events=[{"command": "calculate_stats"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)