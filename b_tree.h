//
// Created by Nataf on 02/06/2020.
//
#ifndef SOFTWARE_VERIF_PROJECT_MASTER_B_TREE_H
#define SOFTWARE_VERIF_PROJECT_MASTER_B_TREE_H
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "alloc_manager.h"


#define CELL_SIZE           256
#define PAGE_SIZE           4096
#define INTERNAL_NODE_MAX_CELLS  500
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;
#define INTERNAL_NODE_CELL_SIZE  (sizeof(uint32_t) + sizeof(uint32_t))
/*
 * Leaf Node Body Layout
 */
typedef struct {
    uint32_t leafNodeKeySize;
    uint32_t leafNodeKeyOffset;
    uint32_t leafNodeValueSize; // = row_size
    uint32_t leafNodeValueOffset; // = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
    uint32_t leafNodeCellSize; // = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
    uint32_t leafNodeSpaceForCells; // = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
    uint32_t leafNodeMaxCells; // LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
    uint32_t leafNodeRightSplitCount;
    uint32_t leafNodeLeftSplitCount;
} NodeBodyLayout;

NodeBodyLayout* new_nbl(int row_size);

uint32_t* leaf_node_num_cells(void* node);

void* leaf_node_cell(void* node, uint32_t cell_num, NodeBodyLayout* nodeBodyLayout);

uint32_t* leaf_node_next_leaf(void* node);

uint32_t * leaf_node_key(void* node, uint32_t cell_num, NodeBodyLayout* nodeBodyLayout);

void* leaf_node_value(void* node, uint32_t cell_num, NodeBodyLayout* nodeBodyLayout);

void initialize_leaf_node(void* node);

NodeType get_node_type(void* node);

void set_node_type(void* node, NodeType type);

uint32_t* internal_node_num_keys(void* node);

uint32_t * internal_node_right_child(void* node);

uint32_t* node_parent(void* node);

uint32_t * internal_node_cell(void *node, uint32_t cell_num);

uint32_t * internal_node_child(void* node, uint32_t child_num);

uint32_t * internal_node_key(void *node, uint32_t key_num);

void initialize_internal_node(void* node);

uint32_t internal_node_find_child(void* node, uint32_t key);

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);

uint32_t get_node_max_key(void *node, NodeBodyLayout* nbl);

bool is_node_root(void* node);

void set_node_root(void* node, bool is_root);
#endif //SOFTWARE_VERIF_PROJECT_MASTER_B_TREE_H
