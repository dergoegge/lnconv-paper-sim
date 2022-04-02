import config
import sys
import logging
import argparse
from plotdescription import (
    PlotDescription,
    PlotDescriptionAction,
    description_from_str,
)
from plot import create_plots
        
ALL_PLOTS = [
    PlotDescription("bandwidth", ["all"]),
    PlotDescription("convdelay", ["all"]),
    PlotDescription("convdelay_cum", ["all"]),
    PlotDescription("waited", ["all"]),
    PlotDescription("payments", ["all"]),
]

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s][%(levelname)s] - %(message)s')

arg_parser = argparse.ArgumentParser(description='Visualize Lightning Network simulation data')
arg_parser.add_argument('configs', metavar='C', type=str, nargs='+',
                    help='Configs to visualize')
arg_parser.add_argument('--plots', type=str, nargs='*', default=ALL_PLOTS, action=PlotDescriptionAction,
                    help='The plots to create')
args = arg_parser.parse_args()

logging.info(f'Visualizing simualtion data for {len(args.configs)} configs')
logging.info(f'Plots: {args.plots}')

configs = []
for config_path in args.configs:
    configs.append(config.Config(config_path))

# check that plot descriptions match the number of configs
for plot in args.plots:
    plot.grab_configs(configs)

logging.debug(configs)

assert(len(configs) > 0)
assert(len(args.plots) > 0)

# create plot
for plot in args.plots:
    create_plots(plot)
