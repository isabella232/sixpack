import requests
import json

home_directory = "/Users/steve/"
experiment_names = "experiment_names.tsv"
test_data = "test_data_activation.tsv"
# Register the experiment in general.


class Experiment():
    def __init__(self, name: str, variants: list, server_address: str='127.0.0.1/'):
        self.name = name
        self.variants = variants
        self.server_address
        self.http_counter = {}
        self.min_date = None
        self.traffic = None
        self.conversions = None


    def load_traffic_data(self, file_name: str):
        try:
            data = json.loads(open(file_name).read())
            assert 'traffic' in data
            assert 'conversions' in data
            self.traffic = data['traffic']
            self.conversions = data['conversions']

    def _build_variant_query_string(self):
        variant_str = ''
        for variant in self.variants:
            variant_str += 'alternatives={variant}&'
        return variant_str


    def register_experiment(self):
        # During testing I found sending a fake event
        # to represent registration resolved some issues.
        end_point = f'{self.server_address}participate?experiment={self.name}'
        variant_str = self._build_variant_query_string()
        end_point+= f'{end_point}&{variant_str}&client_id=fake_user'
        register_expieriment = requests.get(url=end_point)
        print(register_expieriment.status_code, register_expieriment.text)

    def bulk_register_traffic(self):
        for entry in self.traffic:
            if 'customuser_id' not in entry.keys() or 'variant' not in entry.keys() or 'datetime' not in entry.keys():
                print('Skipping entry because its missing keys.')
                continue
            customuser_id = entry['customuser_id']
            variant = entry['variant']
            user_date = entry['datetime']
            variant_str = self._build_variant_query_string()
            participate_endpoint = f'{self.server_address}participate?experiment={self.name}&alternatives={variant_str}force={variant}&record_force=yes&datetime={user_date}&client_id={customuser_id}'
            register_traffic = requests.get(url=participate_endpoint)
            print(register_traffic.status_code, register_traffic.text)

    def bulk_register_conversions(self):
        for entry in self.conversions:
            if 'customuser_id' not in entry.keys() or 'variant' not in entry.keys() or 'datetime' not in entry.keys():
                print('Skipping entry because its missing keys.')
                continue
            customuser_id = entry['customuser_id']
            variant = entry['variant']
            user_date = entry['datetime']
            variant_str = self._build_variant_query_string()
            conversion_endpoint = f'{self.server_address}convert?experiment={self.name}&datetime={user_date}&client_id={customuser_id}'
            conversion_traffic = requests.get(url=conversion_endpoint)
            print(conversion_traffic.status_code, conversion_traffic.text)


def register_experiments(data, base_url):
    experiment_dict = {}
    for line in data:
        line = line.split("\t")
        exp_name = line[0].replace("test: ","")
        exp_name = exp_name.replace(" ","-")
        variant = line[1]
        temp = experiment_dict.get(exp_name, [])
        temp.append(variant)
        experiment_dict[exp_name] = temp

    legal = []
    for k, variants in experiment_dict.items():
        if len(variants) == 1:
            continue
        _temp = f'{base_url}participate?experiment={k}&'
        for variant in variants:
            # print(k, variant)
            _temp += f'alternatives={variant.strip()}&'
        _temp += 'client_id=fake_user'
        print('going to save', k)
        legal.append(k)
        print(_temp, requests.get(url=_temp).text)
        # print(_temp)
    return experiment_dict, legal


def add_traffic(data, base_url):
    for line in data:
        line = line.split("\t")
        user_id = line[0]
        legit_date = line[2]
        if len(legit_date) > 0:
            legit_date = int(line[2])
        else:
            continue
        exp_name = line[5].replace("test: ","")
        experiment_join_date = line[-2]
        if exp_name not in legal:
            continue
        exp_name = exp_name.replace(" ", "-")
        variant_names = experiment_dict[exp_name]
        exp_variant = line[6]
        _temp = ''
        for _variant in variant_names:
            _temp += f'alternatives={_variant}&'
        if legit_date < 3:
            requests.get(url= f'{base_url}participate?experiment={exp_name}&{_temp}force={exp_variant}&record_force=yes&datetime={experiment_join_date}&client_id={user_id}')

def add_conversions(data, base_url):
    for line in data:
        line = line.split("\t")
        user_id = line[0]
        legit_date = line[2]
        if len(legit_date) > 0:
            legit_date = int(legit_date)
        else:
            continue
        exp_name = line[5].replace("test: ","")
        if exp_name not in legal:
            continue
        exp_name = exp_name.replace(" ", "-")
        activated = line[-1].strip()
        experiment_join_date = line[-5]
        if activated == 'legit' and legit_date < 3:
            requests.get(url= f'{base_url}convert?experiment={exp_name}&datetime={experiment_join_date}&client_id={user_id}')

base_url = 'http://localhost:5000/'
experiment_name_data = open(f'{home_directory}{experiment_names}').readlines()
experiment_dict, legal = register_experiments(experiment_name_data, base_url)

test_data = open(f'{home_directory}{test_data}').readlines()
print('There are ', len(test_data), 'entries in tests data. Keeping 5000')
print(legal, 'have been written to the db.')
add_traffic(test_data[1:], base_url)
add_conversions(test_data[1:], base_url)
