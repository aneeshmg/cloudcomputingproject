import re


# This looks good
def generate_results(results_file):
    with open(results_file) as f:
        res = f.readlines()

    results_map = {}

    for line in res:
        if ':' in line and '%' in line:
            line = line.strip()
            obj, confidence = line.split(':')
            obj, confidence = obj.strip(), confidence.strip()
            confidence = int(confidence.split('%')[0])
            if obj in results_map:
                old_confidence = results_map[obj]
                confidence = max(confidence, old_confidence)
                results_map[obj] = confidence
            else:
                results_map[obj] = confidence

    with open(results_file, 'w+') as f:
        f.write(str(results_map))

    return True

generate_results('./sample.bak.txt')
