//
// Created by Nataf on 02/06/2020.
//
#include "b_tree.h"

/*
 * Common Node Header Layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = sizeof(uint8_t) + sizeof(uint8_t);
const uint32_t COMMON_NODE_HEADER_SIZE = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
        +sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
//const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
//                                      +                                       LEAF_NODE_NUM_CELLS_SIZE +
//                                    +                                       LEAF_NODE_NEXT_LEAF_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) +
                                       +sizeof(uint32_t) +
                                       +sizeof(uint32_t);

/*
Internal Node Header Layout
*/
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
//            INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
        sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t)
                                           + sizeof(uint32_t);

/*
 * Internal Node Body Layout
*/
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_CELL_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
//   INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

NodeBodyLayout *new_nbl(int row_size) {
    NodeBodyLayout *nbl = (NodeBodyLayout *) xmalloc(sizeof(NodeBodyLayout));
    nbl->leafNodeKeySize = sizeof(uint32_t);
    nbl->leafNodeKeyOffset = 0;
    nbl->leafNodeValueSize = row_size;
    nbl->leafNodeValueOffset = nbl->leafNodeKeyOffset + nbl->leafNodeKeySize;
    nbl->leafNodeCellSize = nbl->leafNodeKeySize + nbl->leafNodeValueSize;
    nbl->leafNodeSpaceForCells = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
    nbl->leafNodeMaxCells = nbl->leafNodeSpaceForCells / nbl->leafNodeCellSize;
    nbl->leafNodeRightSplitCount = (nbl->leafNodeMaxCells + 1) / 2;
    nbl->leafNodeLeftSplitCount = nbl->leafNodeMaxCells + 1 - nbl->leafNodeRightSplitCount;
    return nbl;
}

uint32_t *leaf_node_num_cells(void *node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * nodeBodyLayout->leafNodeCellSize;
}

uint32_t *leaf_node_next_leaf(void *node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t *leaf_node_key(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return (leaf_node_cell(node, cell_num, nodeBodyLayout));
}

void *leaf_node_value(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return leaf_node_cell(node, cell_num, nodeBodyLayout) + nodeBodyLayout->leafNodeKeySize;
}

void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

NodeType get_node_type(void *node) {
    uint8_t value = *((uint8_t *) (node + NODE_TYPE_OFFSET));
    return (NodeType) value;
}

void set_node_type(void *node, NodeType type) {
    uint8_t value = type;
    *((uint8_t *) (node + NODE_TYPE_OFFSET)) = value;
}

uint32_t *internal_node_num_keys(void *node) {

    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t *internal_node_right_child(void *node) {
    return (node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t *node_parent(void *node) { return node + PARENT_POINTER_OFFSET; }

uint32_t *internal_node_cell(void *node, uint32_t cell_num) {
    return (node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t *internal_node_child(void *node, uint32_t child_num) {
    //child_num is the index of the child we want to get from the node
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys) {
        return internal_node_right_child(node);
    } else {
        return internal_node_cell(node, child_num);
    }
}

uint32_t *internal_node_key(void *node, uint32_t key_num) {
    return (void *) internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

void initialize_internal_node(void *node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

uint32_t internal_node_find_child(void *node, uint32_t key) {
/*
    Return the index of the child which should contain
    the given key.
*/
    uint32_t num_keys = *internal_node_num_keys(node);

    /* Binary search*/
    uint32_t min_index = 0;
    uint32_t max_index = num_keys; /* there is one more child than key */

    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

uint32_t get_node_max_key(void *node, NodeBodyLayout *nbl) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node, *internal_node_num_keys(node) - 1);
        case NODE_LEAF:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1, nbl);
    }
}

bool is_node_root(void *node) {
    uint8_t value = *((uint8_t *) (node + IS_ROOT_OFFSET));
    return (bool) value;
}

void set_node_root(void *node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t *) (node + IS_ROOT_OFFSET)) = value;
}





