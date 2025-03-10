import psycopg2
import re
import json

# Query Plan Parsing for Cardinality Estimation

class Baseline:
    __re_alias_in_condition = re.compile(r'[( ]([A-Za-z0-9_]+)\.')
    __re_and = re.compile(r'\s[Aa][Nn][Dd]\s')
    __re_or = re.compile(r"\s[Oo][Rr]\s")
    __re_filter = re.compile(r'[( ]([A-Za-z0-9_]+\.[A-Za-z0-9_]+)[\s<=>)~]')
    # initialize [sql + plan[format json] + db_name]
    def __init__(self, sql, plan, db_name): 
        self.debug = 0
        # initialize
        self.plan = plan
        self.sql = sql
        self.db_name = db_name
        
        self.parent = {}
        self.children = {}
        self.attributes = {}
        self.aliases = set()
        self.left_deep = True
        self.join_types = {}

        self.card_base = [] # cardiality
        self.card_type1 = []
        self.card_type2 = []
        self.join_counts = {}
        self.alias2rel = {}
        self.__node_index = 0
        self.__joined = set()
        self.join_candidates = []
        self.filter_cols = []
        self.join_cols = []
        self.root = None
        self.hints = None
        self.__join_order_cache = None
        self.__join_order_table_cache = None
        self.__join_method_cache = None
        
        with open("./data/tp_nums.json", 'r') as file :
            self.tp_nums = json.load(file)
            
        if self.plan:
            self.__parse(self.plan)
            self.__make_hints(self.children, self.join_types)
            
        if self.debug:
            print(self.join_cols)
            print(self.filter_cols)
            print(self.hints)
        
        
    def output(self):
        return
    
    def __get_scan_hint(self, type, tb):
        if type == "Seq Scan":
            return "SeqScan({})".format(tb)
        elif type == "Index Scan":
            return "IndexScan({})".format(tb)
        elif type == "Bitmap Heap Scan":
            return "BitmapScan({})".format(tb)
        elif type == "Index Only Scan":
            return "IndexOnlyScan({})".format(tb)
        else:
            return ''
        
    def __get_join_hint(self, type, tbs):
        tbs = " ".join(tbs)
        if type == 0:
            return'NestLoop({})'.format(tbs)
        elif type == 1:
            return 'HashJoin({})'.format(tbs)
        elif type == 2:
            return 'MergeJoin({})'.format(tbs) 
        else:
            return ''
        
    def __make_hints(self, children, join_types):
        if len(children) <= 0:
            # print(self.root)
            scan_type, tb = self.root
            self.hints = self.__get_scan_hint(scan_type, tb)
        else:
            # print(children)
            dic = {}
            leading_dic = {}#index2string
            scan_hints = []
            join_hints = []
            for i in range(len(children) -1, -1, -1):
                left = children[i][0]
                dic[i] = []#记录i节点内的表顺序
                leading_dic[i] = ""
                right = children[i][1]
                if isinstance(left, int):
                    leading_dic[i] += '({})'.format(leading_dic[left])
                    dic[i] += dic[left]
                else:
                    scan_type, tb = left
                    leading_dic[i] += tb
                    dic[i].append(tb)
                    scan_hints.append(self.__get_scan_hint(scan_type, tb))
                if isinstance(right, int):
                    leading_dic[i] += '({})'.format(leading_dic[right])
                    dic[i] += dic[right]
                    # print("int")
                else:
                    scan_type, tb = right
                    leading_dic[i] += ' {}'.format(tb)
                    dic[i].append(tb)
                    scan_hints.append(self.__get_scan_hint(scan_type, tb))
                join_hints.append(self.__get_join_hint(join_types[i], dic[i]))
            self.hints = ""
            for i in scan_hints:
                self.hints += i
                self.hints += '\n'
            for i in join_hints:
                self.hints += i
                self.hints += '\n'
            self.hints += "Leading(({}))".format(leading_dic[0])
    
    @property
    def join_order(self):
        if self.__join_order_cache is not None:
            return list(self.__join_order_cache)
        nodes = []
        stack = [self.root]
        while stack:
            node = stack.pop()
            children = self.children.get(node, None)
            if children:
                nodes.append((node, *children))
                stack.extend(children)
        nodes.reverse()
        dic = {}
        res = []
        for node, left, right in nodes:
            dic[node] = len(dic)
            left = dic.get(left, left)
            right = dic.get(right, right)
            res.append((left, right))
        self.__join_order_cache = res
        return res

    @property
    def join_methods(self):
        if self.__join_method_cache is not None:
            return list(self.__join_method_cache)
        nodes = []
        stack = [self.root]
        while stack:
            node = stack.pop()
            children = self.children.get(node, None)
            if children:
                nodes.append(node)
                stack.extend(children)
        nodes.reverse()
        res = []
        for node in nodes:
            res.append(self.join_types[node])
        self.__join_method_cache = res
        return res

    def __is_parent(self, child, parent):
        _parent = child
        while _parent is not None:
            if _parent == parent:
                return True
            _parent = self.parent.get(_parent, None)
        return False

    @property
    def _result_order(self):
        if self.__join_order_table_cache is not None:
            return list(self.__join_order_table_cache)
        nodes = []
        stack = [self.root]
        while stack:
            node = stack.pop()
            children = self.children.get(node, None)
            if children:
                nodes.append((node, *children))
                stack.extend(children)
        nodes.reverse()
        joined = set(self.join_candidates)
        res = []
        for node, left, right in nodes:
            for left_alias, right_alias in joined:
                if self.__is_parent(left_alias, left) and self.__is_parent(right_alias, right):
                    res.append((left_alias, right_alias))
                    joined.remove((left_alias, right_alias))
                    break
                elif self.__is_parent(right_alias, left) and self.__is_parent(left_alias, right):
                    res.append((right_alias, left_alias))
                    joined.remove((left_alias, right_alias))
                    break
            else:
                # assert False, (nodes, self.join_candidates, (node, left, right))
                break
        if len(res) != len(nodes):
            res = []
        self.__join_order_table_cache = res
        return res

    @property
    def result_order(self):
        return self.join_order

    def __add_condition(self, cond, alias=None):
        if re.search(self.__re_and, cond): # ((t.id = mc.movie_id) AND (it.id = mi_idx.info_type_id))
            conds = re.split(self.__re_and, cond)
            self.__add_condition(conds[0][1:], alias)
            self.__add_condition(conds[1], alias)
            return
        
        if re.search(self.__re_or, cond):# ((t.id = mc.movie_id) OR (it.id = mi_idx.info_type_id))
            conds = re.split(self.__re_or, cond)
            self.__add_condition(conds[0][1:], alias)
            self.__add_condition(conds[1], alias)
            return
        # print(cond)
        # join = [] if alias is None else [alias]
        
        filter_join = []
        filter_join.extend(re.findall(self.__re_filter, cond))
        if self.debug:
            print(filter_join)
        if len(filter_join ) == 1:
            fil_join = filter_join[0].split('.')
            # print(fil_join)
            fil_join[0] = self.alias2rel[fil_join[0]]
            # print(fil_join)
            self.filter_cols.append('.'.join(fil_join))
            # print('.'.join(fil_join))
        if len(filter_join) == 2:
            # the condition might be a filter, e.g.: (info_type_id = 101)
            # return
            if self.debug:
                print(cond)
            left_col = filter_join[0]
            right_col = filter_join[1]
            attr = left_col.split(".")
            attr[0] = self.alias2rel[attr[0]]
            # print(fil_join)
            attr = '.'.join(attr)
            if attr not in self.join_cols:
                self.join_cols.append(attr)
                
            attr = right_col.split(".")
            attr[0] = self.alias2rel[attr[0]]
            # print(fil_join)
            attr = '.'.join(attr)
            if attr not in self.join_cols:
                self.join_cols.append(attr)  
                  
            # # print(join)
            # left, right = filter_join[0], filter_join[1]
            # left_in, right_in = left in self.__joined, right in self.__joined

            # self.join_counts[left] = self.join_counts.get(left, 0) + 1
            # self.join_counts[right] = self.join_counts.get(right, 0) + 1

            # if self.__joined and not left_in and not right_in:
            #     self.left_deep = False ##
            # if not (left_in and right_in):
            #     if left_in:
            #         _jc = (left, right)
            #     else:
            #         _jc = (right, left)
            #     self.join_candidates.append(_jc)
            #     self.__joined.update(_jc)

    def __str(self, node, indent=0):
        if node is None:
            return "()"
        children = self.children.get(node, None)
        if children is None:
            return str(node)
        left, right = self.__str(children[0], indent + 1), self.__str(children[1], indent + 1)
        return f"({node}, {left}, {right})"

    @property
    def __str__(self):
        return self.__str(self.root)
    
    def __attributes(self, plan):
        if 'Actual Rows' in plan:
            actual = plan['Actual Rows'] * plan['Actual Loops']
        else:
            actual = None
        return (
            plan['Plan Rows'],
            plan['Plan Width'],
            actual,
        )

    # parse query plan
    def __parse(self, plan, parent=None, _right=False, _attrs=None):
        alias = plan.get('Alias', None)
        rel_name = plan.get("Relation Name", None)
        if alias is not None:
            self.alias2rel[alias] = rel_name
        node_type = plan['Node Type']
        attrs = self.__attributes(plan) if _attrs is None else _attrs
        if node_type[-4:] == 'Scan':
            # leaf
            alias = plan['Alias']
            if self.debug:
                print(plan)
            assert alias not in self.aliases
            card = plan['Plan Rows']
            
            #use alias as card
            rel_name = self.alias2rel[alias]
            self.card_base.append((alias, card))
            card_1 = ''
            if card >= 1000000:
                card_1 = str(round(card/1000000, 1))+ 'm'
            elif card >= 1000:
                card_1 = str(round(card/1000, 1))+ 'k'
            else:
                card_1 = card
            self.card_type1.append((alias, card_1))
            
            type_dict = ['very low', 'low', 'middle', 'high', 'very high']
            # print(self.tp_nums)
            tb_num = self.tp_nums[self.db_name][rel_name]
            idx = (int)((card-1)/tb_num*5)
            idx = min(4, idx)
            idx = max(0, idx)
            card_2  = type_dict[idx]
            self.card_type2.append((alias, card_2))
            
            
            self.aliases.add(alias)
            self.parent[alias] = parent
            self.attributes[alias] = attrs
            if parent is not None:
                children = self.children.setdefault(parent, [])
                children.append((node_type, alias))
            else:
                self.root = (node_type, alias)
        elif node_type in ('Nested Loop', 'Merge Join', 'Hash Join'):
            # branch
            plans = plan['Plans']
            if _right:
                self.left_deep = False

            left, right = plans
            node_index = self.__node_index
            # print(node_index)
            if node_type == 'Nested Loop':
                self.join_types[node_index] = 0
            elif node_type == 'Hash Join':
                self.join_types[node_index] = 1
            elif node_type == 'Merge Join':
                self.join_types[node_index] = 2
            else:
                self.join_types[node_index] = -1

            self.parent[node_index] = parent
            self.attributes[node_index] = attrs
            if parent is not None:
                children = self.children.setdefault(parent, [])
                children.append(node_index)
            else:
                self.root = node_index

            self.__node_index += 1
            self.__parse(left, parent=node_index)
            self.__parse(right, parent=node_index, _right=True)

        else:
            # others
            plans = plan['Plans']

            self.__parse(plans[0], parent=parent, _right=_right, _attrs=attrs)
        
        
        conditioned = False
        filter = plan.get("Filter", None)
        if filter is not None:
            # print("filter")
            alias = plan.get('Alias', None)
            if alias is not None:
                self.__add_condition(filter, alias)
        cond = plan.get('Recheck Cond', None)
        if cond is not None:
            # print("recheck cond")
            if alias is not None and cond.find('=') >= 0:
                conditioned = True
                self.__add_condition(cond, alias)
        cond = plan.get('Index Cond', None)
        if cond is not None:
            if alias is not None and cond.find('=') >= 0:
                conditioned = True
                self.__add_condition(cond, alias)
        cond = plan.get('Hash Cond', None)
        if cond is not None:
            conditioned = True
            self.__add_condition(cond)
        cond = plan.get('Merge Cond', None)
        if cond is not None:
            conditioned = True
            self.__add_condition(cond)
        cond = plan.get('Join Filter', None)
        if cond is not None:
            conditioned = True
            self.__add_condition(cond)

    def mapping(self, plan):
        base_to_plan = {a : a for a in plan.sql.aliases}
        plan_to_base = dict(base_to_plan)
        def traverse(node, visited):
            visited.add(node)
            parent = self.parent.get(node, None)
            if parent is None:
                return
            plan_node = base_to_plan.get(node, None)
            if plan_node is not None:
                plan_parent = plan.direct_parent.get(plan_node, None)
                if plan_parent is not None:
                    base_to_plan[parent] = plan_parent
                    plan_to_base[plan_parent] = parent
            traverse(parent, visited=visited)
        visited = set()
        for alias in self.aliases:
            traverse(alias, visited)
        return base_to_plan, plan_to_base

    def mapping_attrs(self, plan):
        base_to_plan, plan_to_base = self.mapping(plan)
        res = {}
        for alias in plan.sql.aliases:
            res[alias] = self.attributes[alias]
        for alias in range(plan.total_branch_nodes):
            res[alias] = self.attributes[plan_to_base[alias]]
        return res
