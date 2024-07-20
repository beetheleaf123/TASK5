import os
import random
import time
from functools import lru_cache
from multiprocessing import Pool, cpu_count

def generate_matrix(rows, columns):
    """Generate a matrix with random '1's and '0's."""
    return ''.join(random.choice('01') for _ in range(rows * columns))

def create_mat_in_file(directory, filename, num_matrices):
    """
    Create the mat.in file with randomly generated matrices.
    Each matrix will have dimensions between 5x5 and 10x10.
    The number of matrices is specified by num_matrices.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, filename)
    with open(file_path, 'w') as file:
        for _ in range(num_matrices):
            rows = random.randint(5, 10)
            columns = random.randint(5, 10)
            matrix_data = generate_matrix(rows, columns)
            file.write(f'{rows}x{columns}:{matrix_data}\n')

def read_matrices_from_file(filename):
    matrices = []
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            dim_part, matrix_data = line.split(':')
            rows, columns = map(int, dim_part.split('x'))
            matrix = []
            for i in range(rows):
                row = tuple(matrix_data[i * columns:(i + 1) * columns])
                matrix.append(row)
            matrices.append(tuple(matrix))
    return matrices

@lru_cache(maxsize=1000)
def is_isolated_cluster(matrix, cluster):
    rows = len(matrix)
    columns = len(matrix[0])
    for r, c in cluster:
        directions = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
        for dr, dc in directions:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < columns and (nr, nc) not in cluster:
                if matrix[nr][nc] == '1':
                    return False
    return True

def dfs(matrix, rows, columns, r, c, cluster_size, cluster):
    if r < 0 or r >= rows or c < 0 or c >= columns or matrix[r][c] == '0' or (r, c) in cluster:
        return
    cluster.add((r, c))
    if len(cluster) == cluster_size:
        return
    directions = [(-1, 0), (1, 0), (0, -1), (0, 1)]
    for dr, dc in directions:
        nr, nc = r + dr, c + dc
        dfs(matrix, rows, columns, nr, nc, cluster_size, cluster)

@lru_cache(maxsize=1000)
def find_clusters(matrix, size):
    rows = len(matrix)
    columns = len(matrix[0])
    clusters = set()

    for r in range(rows):
        for c in range(columns):
            if matrix[r][c] == '1':
                cluster = set()
                dfs(matrix, rows, columns, r, c, size, cluster)
                if len(cluster) == size and is_isolated_cluster(matrix, tuple(cluster)):
                    clusters.add(frozenset(cluster))
    return len(clusters)

@lru_cache(maxsize=1000)
def count_isolated_ones(matrix):
    isolated_count = 0
    for row in range(len(matrix)):
        for col in range(len(matrix[0])):
            if matrix[row][col] == '1' and is_isolated(matrix, row, col):
                isolated_count += 1
    return isolated_count

@lru_cache(maxsize=1000)
def is_isolated(matrix, row, col):
    rows = len(matrix)
    columns = len(matrix[0])
    directions = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
    for dr, dc in directions:
        r, c = row + dr, col + dc
        if 0 <= r < rows and 0 <= c < columns:
            if matrix[r][c] == '1':
                return False
    return True

def process_file(file_info):
    input_file, output_dir = file_info
    matrices = read_matrices_from_file(input_file)
    results = []

    for matrix in matrices:
        isolated_ones = count_isolated_ones(matrix)
        clusters_of_two = find_clusters(matrix, 2)
        clusters_of_three = find_clusters(matrix, 3)
        results.append((isolated_ones, clusters_of_two, clusters_of_three))

    output_file = os.path.join(output_dir, os.path.basename(input_file).replace('.in', '.out'))
    with open(output_file, 'w') as file:
        for isolated_ones, clusters_of_two, clusters_of_three in results:
            file.write(f'{isolated_ones} {clusters_of_two} {clusters_of_three}\n')

def main(input_dir, output_dir, num_files, num_matrices_per_file):
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate test files
    for i in range(num_files):
        create_mat_in_file(input_dir, f'mat{i}.in', num_matrices_per_file)

    file_list = [os.path.join(input_dir, f'mat{i}.in') for i in range(num_files)]

    start_time = time.time()

    with Pool(cpu_count()) as pool:
        pool.map(process_file, [(file, output_dir) for file in file_list])

    end_time = time.time()
    print(f"Total runtime: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    input_dir = 'R:\\input'  # Replace 'R' with the actual drive letter of your RAM disk
    output_dir = 'R:\\output'  # Replace 'R' with the actual drive letter of your RAM disk
    num_files = 50  # Number of files to generate and process for testing
    num_matrices_per_file = 100000  # Number of matrices per file

    main(input_dir, output_dir, num_files, num_matrices_per_file)
