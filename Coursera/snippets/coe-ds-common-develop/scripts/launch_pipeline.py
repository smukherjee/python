from utils.aws import launch_pipeline_on_emr, parse_launch_pipeline_on_emr

if __name__ == '__main__':
    args = parse_launch_pipeline_on_emr()
    launch_pipeline_on_emr(class_name=args.class_name, process_group=args.process_group, dt_start=args.dt_start,
                           until=args.until, n_rounds=args.n_rounds, environment=args.environment, run=args.run)
