#[derive(Debug, Default)]
struct BPTreeKeyValue {
    key: String,
    value: String,
}

#[derive(Debug)]
enum BPTreeNode {
    Internal {
        parent: Option<usize>,
        child: Vec<usize>,
        keys: Vec<String>,
    },
    Leaf {
        parent: Option<usize>,
        next: Option<usize>,
        kvs: Vec<BPTreeKeyValue>,
    },
}


impl BPTreeNode {
    pub fn split(&mut self) -> BPTreeNode {
        // 该分裂仅将节点内部数据平分, 并不涉及父节点的连锁反应
        match self {
            BPTreeNode::Internal { parent, child, keys } => {
                // 分裂 Internal 节点
                let mut center_and_right_key = keys.split_off(child.len() / 2);
                BPTreeNode::Internal {
                    parent: parent.clone(),
                    child: child.split_off(child.len() / 2 + 1),
                    keys: center_and_right_key.split_off(1),
                }
            }
            BPTreeNode::Leaf { parent, kvs, .. } => {
                // 分裂 Leaf 节点
                BPTreeNode::Leaf {
                    parent: parent.clone(),
                    next: None,
                    kvs: kvs.split_off(kvs.len() / 2),
                }
            }
        }
    }

    pub fn set_parent_offset(&mut self, offset: usize) -> usize {
        match self {
            BPTreeNode::Internal { parent, .. } => {
                parent.insert(offset).clone()
            }
            BPTreeNode::Leaf { parent, .. } => {
                parent.insert(offset).clone()
            }
        }
    }

    pub fn push_data(&mut self, new_child: usize, key: String) {
        if let BPTreeNode::Internal {
            child,
            keys,
            ..
        } = self {
            if let Err(idx) = keys.binary_search_by(|_k| _k.cmp(&key)) {
                keys.insert(idx, key);
                child.insert(idx + 1, new_child);
            }
        }
    }
}

#[derive(Debug)]
struct BPTree {
    // BTree 是一种多路搜索树, order 对应着形状, 也就是对应的路数, 或者说是节点中指针的数量
    // 因为节点中是指针和数据间隔排列, 因此节点中可存放的数据有以下规则
    // 最多可存放元素 order - 1, 最少可存放 (order / 2) 向上取整后 -1 个
    order: usize,
    nodes: Vec<BPTreeNode>,
    root: usize,
    first_leaf: usize,
}

impl BPTree {
    pub fn new(order: usize) -> Self {
        let order = if order % 2 == 0 {
            // 一个节点填满元素后, 将从中间分裂开成两个节点, 那么 order 是偶数时
            // 元素会是奇数个, 此时与奇数的情况类似, 只有某些个别地方需要单独做处理
            // 所以这里舍弃 order 是偶数的情况以简化实现
            order + 1
        } else if order < 3 {
            // order 小于 3 的时候, 与正常二叉树一致, 所以无意义
            3
        } else {
            order
        };
        let mut nodes = Vec::<BPTreeNode>::new();
        nodes.push(BPTreeNode::Leaf {
            parent: None,
            next: None,
            kvs: vec![],
        });
        Self {
            order,
            nodes,
            root: 0,
            first_leaf: 0,
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        let kv = BPTreeKeyValue { key, value };
        // 查找
        let leaf_offset = Self::search_leaf(&self.nodes, self.root, &kv.key);
        // 插入
        if let Some(new_root) = Self::insert(&mut self.nodes, kv, leaf_offset, self.order) {
            self.root = new_root;
        }
    }

    fn insert(nodes: &mut Vec<BPTreeNode>, kv: BPTreeKeyValue, leaf_offset: usize, order: usize) -> Option<usize> {
        if let Some(BPTreeNode::Leaf { kvs, .. }) = nodes.get(leaf_offset) {
            if kvs.len() == order - 1 {
                // 分裂节点
                let new_leaf_offset = if
                let Some(old_node) = nodes.get_mut(leaf_offset) {
                    let new_node = old_node.split();
                    nodes.push(new_node);
                    nodes.len() - 1
                } else { return None; };

                return Self::insert_full(nodes, kv, leaf_offset, new_leaf_offset, order);
            } else if let Some(BPTreeNode::Leaf { kvs, .. }) = nodes.get_mut(leaf_offset) {
                Self::insert_non_full(kvs, kv);
                return None;
            }
        }
        return None;
    }

    fn insert_full(
        nodes: &mut Vec<BPTreeNode>,
        kv: BPTreeKeyValue,
        old_leaf_offset: usize,
        new_leaf_offset: usize,
        order: usize,
    ) -> Option<usize> {
        let mut _parent: Option<usize> = None;
        let mut _key;

        // 处理节点中的数据
        if let [old_leaf, .., new_leaf] = &mut nodes[old_leaf_offset..=new_leaf_offset] {
            // 解构
            let BPTreeNode::Leaf {
                parent: new_parent,
                next: new_next,
                kvs: new_kvs
            } = new_leaf else { return None; };

            let BPTreeNode::Leaf {
                parent: old_parent,
                next: old_next,
                kvs: old_kvs
            } = old_leaf else { return None; };

            _parent = old_parent.clone();

            *new_parent = _parent.clone();
            *new_next = old_next.clone();
            *old_next = Some(new_leaf_offset);

            _key = new_kvs[0].key.clone();

            if _key > kv.key {
                Self::insert_non_full(old_kvs, kv);
            } else {
                Self::insert_non_full(new_kvs, kv);
            }
        } else { return None; }

        // 将分裂的节点插入父节点中
        if _parent == None {
            // 如果没有则新建
            let new_parent = BPTreeNode::Internal {
                parent: None,
                child: vec![old_leaf_offset, new_leaf_offset],
                keys: vec![_key],
            };
            nodes.push(new_parent);
            let new_root_offset = nodes.len() - 1;

            if let [old_leaf, .., new_leaf] = &mut nodes[old_leaf_offset..=new_leaf_offset] {
                if let BPTreeNode::Leaf { parent: new_root, .. } = new_leaf {
                    *new_root = Some(new_root_offset);
                }
                if let BPTreeNode::Leaf { parent: old_root, .. } = old_leaf {
                    *old_root = Some(new_root_offset);
                }
            }
            return Some(new_root_offset);
        }
        // 循环处理父节点
        Self::split_nodes(
            nodes,
            Some(new_leaf_offset),
            Some(_key),
            _parent,
            order,
        )
    }

    fn split_nodes(
        nodes: &mut Vec<BPTreeNode>,
        right_leaf_offset: Option<usize>,
        right_leaf_key: Option<String>,
        parent: Option<usize>,
        order: usize,
    ) -> Option<usize> {
        // 子节点会传上来一个分裂后的右节点的 key 和 索引
        // 如果没有传上来的元素, 则父节点无变化, 此时分裂完毕
        let mut curr_parent_offset: Option<usize> = parent;
        let mut new_right_child_offset = right_leaf_offset;
        let mut new_right_key = right_leaf_key;
        while new_right_child_offset != None {
            // 解构, 取得第一个可变引用
            let Some(parent_node) = (if let Some(parent_offset) = curr_parent_offset {
                nodes.get_mut(parent_offset)
            } else { break; }) else { break; };
            let BPTreeNode::Internal {
                parent, keys, ..
            } = parent_node else { break; };
            let next_parent = parent.clone();

            // 节点元素是否已满
            if keys.len() == order - 1 {
                // 先找到中间的 key 扔给父节点
                let center_key = keys[order / 2].clone();
                // 如果没有父节点了, 说明已经是最后一个
                if *parent == None {
                    // 新建一个父节点, 插入得到它的索引
                    // 父节点的左节点的索引为应该被分裂的节点
                    let Some(old_node_offset) = curr_parent_offset else { break; };
                    let _new_root_node = BPTreeNode::Internal {
                        parent: None,
                        child: vec![old_node_offset.clone()],
                        keys: vec![],
                    };
                    nodes.push(_new_root_node);
                    let new_root_node_offset = nodes.len() - 1;

                    // 获取原左节点
                    let mut right_node = if let Some(left_node)
                        = nodes.get_mut(old_node_offset) {
                        // 设置左节点的父节点, 分裂
                        left_node.set_parent_offset(new_root_node_offset);
                        left_node.split()
                    } else { break; };

                    // 插入右节点的数据
                    let new_child_offset = if let (Some(child_offset), Some(key))
                        = (new_right_child_offset.clone(), new_right_key.clone()) {
                        right_node.set_parent_offset(new_root_node_offset);
                        right_node.push_data(child_offset, key);
                        nodes.push(right_node);
                        nodes.len() - 1
                    } else { break; };

                    // 更新右节点的子节点
                    Self::update_child_parent(nodes, new_child_offset);

                    // 设置新的父节点
                    new_right_child_offset = Some(new_child_offset);
                    curr_parent_offset = Some(new_root_node_offset);
                } else {
                    // 分裂原节点
                    let Some(parent_offset) = parent.clone() else { break; };
                    let mut _new_node = parent_node.split();

                    // 插入数据
                    let Some(child_idx) = new_right_child_offset.clone() else { break; };
                    let Some(key) = new_right_key.clone() else { break; };
                    _new_node.set_parent_offset(parent_offset.clone());
                    _new_node.push_data(child_idx, key);
                    nodes.push(_new_node);
                    let new_child_offset = nodes.len() - 1;

                    // 更新右节点的子节点
                    Self::update_child_parent(nodes, new_child_offset);

                    new_right_child_offset = Some(new_child_offset);
                    curr_parent_offset = Some(parent_offset);
                }
                new_right_key = Some(center_key);
            } else {
                // 没有新数据需要更新
                let Some(child_offset) = new_right_child_offset.clone() else { break; };
                let Some(key) = new_right_key.clone() else { break; };

                parent_node.push_data(child_offset, key);

                // 如果这是最后一个节点
                if next_parent == None {
                    return curr_parent_offset;
                }

                curr_parent_offset = next_parent;
                new_right_child_offset = None;
                new_right_key = None;
            }
        }
        return None;
    }

    fn insert_non_full(kvs: &mut Vec<BPTreeKeyValue>, kv: BPTreeKeyValue) {
        match kvs.binary_search_by(|_kv| _kv.key.cmp(&kv.key)) {
            Ok(idx) => {
                // 已存在则更新
                let old_kv = &mut kvs[idx];
                if kv.key == old_kv.key {
                    old_kv.value = kv.value;
                }
            }
            Err(idx) => {
                // 不存在则插入
                kvs.insert(idx, kv);
            }
        }
    }

    pub fn get(&self, key: &String) -> Option<&BPTreeKeyValue> {
        let leaf_offset = Self::search_leaf(&self.nodes, self.root, key);
        if let Some(BPTreeNode::Leaf { kvs, .. }) = self.nodes.get(leaf_offset) {
            match kvs.binary_search_by(|_k| _k.key.cmp(key)) {
                Ok(idx) => { kvs.get(idx) }
                Err(_) => None
            }
        } else {
            None
        }
    }

    fn search_leaf(nodes: &Vec<BPTreeNode>, root_offset: usize, key: &String) -> usize {
        // 按照 key 从 root 开始搜索叶子节点
        let mut offset = root_offset;
        while let Some(BPTreeNode::Internal { keys, child, .. }) = nodes.get(offset) {
            match keys.binary_search_by(|_k| _k.cmp(key)) {
                Ok(idx) => { offset = child[idx] + 1 }
                Err(idx) => { offset = child[idx] }
            }
        }
        offset
    }

    fn update_child_parent(nodes: &mut Vec<BPTreeNode>, new_child_idx: usize) {
        // 更新子节点的父节点
        let BPTreeNode::Internal { child, .. } = &nodes[new_child_idx] else { return; };
        let childs = child.clone();
        for i in 0..childs.len() {
            let child_idx = childs[i];
            match &mut nodes[child_idx] {
                BPTreeNode::Internal { parent, .. } => {
                    *parent = Some(new_child_idx);
                }
                BPTreeNode::Leaf { parent, .. } => {
                    *parent = Some(new_child_idx);
                }
            }
        }
    }
}

fn main() {
    println!("--------------------- 创建 (1 Leaf)");
    let mut b = BPTree::new(5);
    b.put("d".to_string(), "1".to_string());
    b.put("a".to_string(), "1".to_string());
    b.put("b".to_string(), "1".to_string());
    for i in 0..b.nodes.len() {
        println!("\n{}: {:?}", i, &b.nodes[i]);
    }
    println!("\nroot: {:?}\n", b.root);

    println!("--------------------- 插入时更新 {{'d': 2}} (1 Leaf)");
    b.put("d".to_string(), "2".to_string());
    for i in 0..b.nodes.len() {
        println!("\n{}: {:?}", i, &b.nodes[i]);
    }
    println!("\nroot: {:?}\n", b.root);

    println!("--------------------- 第 1 次分裂 (2 Leaf, 1 Internal)");
    b.put("e".to_string(), "1".to_string());
    b.put("c".to_string(), "1".to_string());
    for i in 0..b.nodes.len() {
        println!("\n{}: {:?}", i, &b.nodes[i]);
    }
    println!("\nroot: {:?}\n", b.root);

    println!("--------------------- 第 2 次分裂 (3 Leaf, 1 Internal)");
    b.put("f".to_string(), "1".to_string());
    b.put("g".to_string(), "1".to_string());
    b.put("h".to_string(), "1".to_string());
    for i in 0..b.nodes.len() {
        println!("\n{}: {:?}", i, &b.nodes[i]);
    }
    println!("\nroot: {:?}\n", b.root);

    println!("--------------------- 第 3 次分裂 (4 Leaf, 1 Internal)");
    b.put("i".to_string(), "7".to_string());
    b.put("j".to_string(), "1".to_string());
    for i in 0..b.nodes.len() {
        println!("\n{}: {:?}", i, &b.nodes[i]);
    }
    println!("\nroot: {:?}", b.root);

    println!("--------------------- 第 4 次分裂 (5 Leaf, 1 Internal)");
    b.put("k".to_string(), "1".to_string());
    b.put("l".to_string(), "1".to_string());
    for i in 0..b.nodes.len() {
        let leaf = &b.nodes[i];
        println!("\n{}: {:?}", i, leaf);
    }
    println!("\nroot: {:?}", b.root);

    println!("--------------------- 第 5 次分裂 (6 Leaf, 3 Internal)");
    b.put("m".to_string(), "9".to_string());
    b.put("n".to_string(), "1".to_string());
    for i in 0..b.nodes.len() {
        println!("\n{}: {:?}", i, &b.nodes[i]);
    }
    println!("\nroot: {:?}", b.root);
}
