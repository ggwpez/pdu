import json
import argparse
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

plt.style.use('dark_background')

def create_storage_text(data):
    storage_by_pallet = defaultdict(list)
    items_by_pallet = defaultdict(list)
    
    for pallet in data['pallets']:
        if 'storage' in pallet:
            for item in pallet['storage']:
                storage_by_pallet[pallet['name']].append(
                    (item['key_len'] + item['value_len'], item['name'])
                )
        if 'items' in pallet:
            for item in pallet['items']:
                items_by_pallet[pallet['name']].append(
                    (item['key_len'] + item['value_len'], item['name'])
                )
    
    text_lines = []
    total_size = sum(p['size'] for p in data['pallets'])
    
    for pallet in sorted(data['pallets'], key=lambda x: x['size'], reverse=True):
        pname = pallet['name']
        psize = pallet['size']
        if (psize/total_size)*100 <= 1:
            continue
            
        text_lines.append(f"{pname} ({psize/1024/1024:.1f} MiB)")
        
        if pname in storage_by_pallet:
            for size, name in sorted(storage_by_pallet[pname], key=lambda x: x[0], reverse=True):
                if size >= 100 * 1024:  # 100 KiB
                    text_lines.append(f" {name} ({size/1024/1024:.1f} MiB)")
                
        if pname in items_by_pallet:
            for size, name in sorted(items_by_pallet[pname], key=lambda x: x[0], reverse=True):
                if size >= 100 * 1024:  # 100 KiB
                    text_lines.append(f" {name} ({size/1024/1024:.1f} MiB)")
                
        text_lines.append("")
    
    return "\n".join(text_lines)

def create_pie(data):
    pallet_sizes = sorted([(p['size'], p['name']) for p in data['pallets']], reverse=True)
    total = sum(size for size, _ in pallet_sizes)
    
    main_items = [(size, name) for size, name in pallet_sizes if (size/total)*100 > 1]
    other_sum = sum(size for size, name in pallet_sizes if (size/total)*100 <= 1)
    
    sizes = [size for size, _ in main_items]
    labels = [f"{name} {size/1024/1024:.0f}" for size, name in main_items]
    
    if other_sum > 0:
        sizes.append(other_sum)
        labels.append(f"Other {other_sum/1024/1024:.0f}")
    
    fig = plt.figure(figsize=(15, 8))
    fig.patch.set_facecolor('#1C1C1C')
    
    text_ax = plt.axes([0.02, 0.02, 0.3, 0.95])
    text_ax.text(0, 1, create_storage_text(data), 
                fontfamily='monospace',
                va='top', 
                color='white',
                transform=text_ax.transAxes)
    text_ax.axis('off')
    
    pie_ax = plt.axes([0.35, 0.02, 0.6, 0.95])
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(sizes)))
    wedges, _ = pie_ax.pie(sizes, colors=colors, radius=1.4)
    pie_ax.pie(sizes, labels=labels, colors=colors, autopct=lambda _: '', pctdistance=0.85, labeldistance=0.9)
    pie_ax.set_title(f"Pallet sizes for {data['network'].capitalize()} [MiB]", color='white', pad=40, fontsize=16)
    
    plt.show()

def main(input_file):
    with open(input_file, "r") as f:
        data = json.load(f)
    create_pie(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="Input JSON file")
    args = parser.parse_args()
    main(args.input)
