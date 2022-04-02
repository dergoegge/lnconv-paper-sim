import argparse

# format: "name:[all|(c_a, c_b, c_c, ...)|(c_x, c_y)]"
class PlotDescription():
    def __init__(self, name, relations):
        self.name = name
        self.relations = relations
        self.configs = {}

    def relations_str(self):
        out = "["
        for rel in self.relations:
            out += str(rel).replace(" ", "") + "|"
        out = out[:-1]
        out += "]"
        return out
    
    def relation_configs(self):
        rel_confs = []
        for rel in self.relations:
            confs = []
            if rel == "all":
                for i in range(len(self.configs.keys())):
                    confs.append(self.configs[i])
            else:
                for i in rel:
                    confs.append(self.configs[i])
            rel_confs.append(confs) 
        return rel_confs


    def grab_configs(self, configs):
        self.check(len(configs))
        for rel in self.relations:
            if rel == "all":
                for i in range(len(configs)):
                    self.configs[i] = configs[i]
                break
            else:
                for i in rel:
                    self.configs[i] = configs[i]
 
    def check(self, num_configs):
        for rel in self.relations:
            if rel == "all":
                continue
            else:
                for i in rel:
                    assert(i < num_configs)

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return self.name + ":" + self.relations_str()

def description_from_str(str_in):
    relations = []

    parts = str_in.split(":")
    assert(len(parts) > 1)
    name = parts[0]

    rels = parts[1][1:-1]
    for rel in rels.split("|"):
        if rel == "all":
            relations.append("all")
            continue

        assert(rel.startswith("(") and rel.endswith(")"))
        str_config_ids = rel[1:-1]
        config_ids = tuple(map(lambda k: int(k), str_config_ids.split(",")))
        relations.append(config_ids)

    return PlotDescription(name, relations)

class PlotDescriptionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if type(values) is list:
            plots = [description_from_str(v) for v in values]
        else:
            plots = description_from_str(values)
        setattr(namespace, self.dest, plots)
