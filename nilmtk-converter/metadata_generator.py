from yaml import load, dump
from os.path import join, isdir, isfile
metadata_path = 'metadata/'
from os import listdir

def yaml_dump(data, file_name):
    f = open(file_name, 'w+')
    dump(data, f, allow_unicode=True, default_style = None,
         default_flow_style=False, sort_keys=False)
    f.close()

if __name__== '__main__':
    
    # dataset.yaml
    dataset_dict = {'name': 'ENERTALK',
                   'long_name': 'The ENERTALK Data set',
                   'creators': ['Shin, Changho', 'Lee, Eunjung', 'Han, Jungyun',
                               'Yim, Jaeryun', 'Rhee, Wonjong', 'Lee, Hyoseop'],
                    'contact': ['chshin@encoredtech.com', 'hslee@encoredtech.com'],
                    'description': 'Several weeks of power data for 22 different Korean homes with 15 Hz resolution',
                    'number_of_buildings': 22,
                    'timezone': 'Asia/Seoul',
                    'geo_location':{
                        'locality': 'Seoul',
                        'country': 'KR',
                        'latitude': 37.5080615,
                        'longitude': 127.03555000000006
                    },
                    'schema': 'https://github.com/nilmtk/nilm_metadata/tree/v0.2'
                   }

    yaml_dump(dataset_dict, join(metadata_path, 'dataset.yaml'))


    # meter_devices.yaml
    meter_devices_dict = {
        'ENERTALK': {
            'model': 'EDM3KR',
            'manufacturer': 'ENCORED',
            'manufacturer_url': 'https://www.enertalk.com/',
            'sample_period': 1/15,
            'measurements':
                [{
                    'physical_quantity': 'power',
                    'type': 'active',
                    'upper_limit': 50000,
                    'lower_limit': 0
                },
                {
                    'physical_quantity': 'power',
                    'type': 'reactive',
                    'upper_limit': 50000,
                    'lower_limit': -50000
                }]
            ,
            'wireless': True
        },

        'ENERTALK PLUG': {
            'model': 'EDM3ES',
            'manufacturer': 'ENCORED',
            'manufacturer_url': 'https://www.enertalk.com/',
            'sample_period': 1/15, # 15 Hz
            'measurements':
                [{
                    'physical_quantity': 'power',
                    'type': 'active',
                    'upper_limit': 10000,
                    'lower_limit': 0
                },
                {
                    'physical_quantity': 'power',
                    'type': 'reactive',
                    'upper_limit': 10000,
                    'lower_limit': -10000
                }]
            ,
            'wireless': False
        }
    }

    yaml_dump(meter_devices_dict, join(metadata_path, 'meter_devices.yaml'))


    # building 1~22
    input_path = '../enertalk-dataset'
    house_list = [fname for fname in listdir(input_path) if not fname.startswith('.')]

    app_type_mapping = {
        'washing-machine': 'washing machine',
        'TV': 'television',
        'rice-cooker': 'rice cooker',
        'microwave': 'microwave',
        'fridge': 'fridge freezer',
        'kimchi-fridge': 'fridge',
        'water-purifier': 'water filter',
    }
    for house in sorted(house_list):

        meter_num_app_dict = {}
        app_name_set = set()
        # elec_meters
        date_list = sorted(listdir(join(input_path, house)))
        for date in date_list:
            fname_list = sorted(listdir(join(input_path, house, date)))
            for fname in fname_list:
                app_num = int(fname.split('_')[0]) + 1
                app_name = fname.split('_')[1].split('.')[0]
                meter_num_app_dict[app_num] = app_name

                if app_name!='total':
                    app_name_set.add(app_name)

        elec_meters = {}
        for key in sorted(meter_num_app_dict.keys()):
            if key==1:
                elec_meters[key] = {'site_meter': True,
                                 'device_model': 'ENERTALK'}
            else:
                elec_meters[key] = {'submeter_of': 0,
                                   'device_model': 'ENERTALK PLUG'}

        # appliances
        app_instance_dict = {}
        for app in app_name_set:
            app_instance_dict[app] = {}
            for key in sorted(meter_num_app_dict.keys()):
                app_name = meter_num_app_dict[key]
                if app == app_name:
                    app_instance_dict[app][key] = len(app_instance_dict[app].keys())+1

        apps = []
        for key in sorted(meter_num_app_dict.keys()):
            app_name = meter_num_app_dict[key]
            meters_str = [key]
            if app_name == 'total':
                continue

            if len(app_instance_dict[app_name].keys())>1:
                app_instance = {
                    'original_name': app_name,
                    'type': app_type_mapping[app_name],
                    'instance': app_instance_dict[app_name][key],
                    'multiple': True, 
                    'meters': meters_str
                }
            else:
                app_instance = {
                    'original_name': app_name,
                    'type': app_type_mapping[app_name],
                    'instance': app_instance_dict[app_name][key],
                    'meters': meters_str
                }
            apps.append(app_instance)

        building_num = int(house) + 1

        building_dict = {
            'instance': building_num,
            'original_name': 'house_'+house,
            'elec_meters': elec_meters,
            'appliances': apps
        }

        yaml_dump(building_dict,  join(metadata_path, 'building'+str(building_num)+'.yaml'))





