#include "include/data_graph.h"

namespace daf {
DataGraph::DataGraph(const std::string& filename) : Graph(filename) {}

DataGraph::~DataGraph() {
  delete[] transferred_label_;
  delete[] adj_offs_by_label_;
  delete[] offs_by_label_;
  delete[] vertices_sorted_;
  delete[] linear_nbr_bitset_;
  delete[] max_nbr_degree_;
}

void DataGraph::LoadAndProcessGraph(std::unordered_map<std::string, int>& label2id_mapping) {
  std::vector<std::vector<Vertex>> adj_list;
  std::unordered_map<Label, Label> transferred_label_map;
  Label max_label = 0;

  LoadRoughGraph(&adj_list, label2id_mapping);

  // transfer label & get sorted degrees (for constructing C_ini(u))
  Label cur_transferred_label = 0;

  vertices_sorted_ = new Vertex[GetNumVertices()];

  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    vertices_sorted_[v] = v;
    Label l = label_[v];
    if (l > max_label) max_label = l;
    if (transferred_label_map.find(l) == transferred_label_map.end()) {
      transferred_label_map[l] = cur_transferred_label;
      cur_transferred_label += 1;
    }
    label_[v] = transferred_label_map[l];
  }

  std::sort(vertices_sorted_, vertices_sorted_ + GetNumVertices(),
            [this, &adj_list](Vertex v1, Vertex v2) -> bool {
              if (GetLabel(v1) != GetLabel(v2))
                return GetLabel(v1) < GetLabel(v2);
              else
                return adj_list[v1].size() > adj_list[v2].size();
            });

  num_label_ = transferred_label_map.size();

  label_frequency_ = new Size[num_label_];
  std::fill(label_frequency_, label_frequency_ + num_label_, 0);

  max_label_frequency_ = 0;

  transferred_label_ = new Label[max_label + 1];
  // transferred_label_[l] = INVALID_LB iff there is no label l in data graph
  std::fill(transferred_label_, transferred_label_ + max_label + 1, INVALID_LB);

  for (auto p : transferred_label_map) {
    transferred_label_[p.first] = p.second;
  }

  nbr_bitset_size_ = (GetNumLabels() - 1) / (sizeof(uint64_t) * CHAR_BIT) + 1;
  linear_nbr_bitset_ = new uint64_t[GetNumVertices() * nbr_bitset_size_];
  std::fill(linear_nbr_bitset_,
            linear_nbr_bitset_ + GetNumVertices() * nbr_bitset_size_, 0ull);

  max_nbr_degree_ = new Size[GetNumVertices()];
  std::fill(max_nbr_degree_, max_nbr_degree_ + GetNumVertices(), 0);

  // compute offsets & construct adjacent list and label frequency
  Size cur_idx = 0;
  max_degree_ = 0;
  linear_adj_list_ = new Vertex[GetNumEdges() * 2];
  start_off_ = new Size[GetNumVertices() + 1];
  offs_by_label_ = new Size[GetNumLabels() + 1];
  adj_offs_by_label_ =
      new std::pair<Size, Size>[GetNumVertices() * GetNumLabels()];
  core_num_ = new Size[GetNumVertices()];

  Label cur_label = 0;  // min label of data graph is 0
  offs_by_label_[0] = 0;
  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    Size start = v * GetNumLabels();
    label_frequency_[GetLabel(v)] += 1;
    start_off_[v] = cur_idx;

    if (label_frequency_[GetLabel(v)] > max_label_frequency_) {
      max_label_frequency_ = label_frequency_[GetLabel(v)];
    }

    Label label_sorted = GetLabel(vertices_sorted_[v]);
    if (label_sorted != cur_label) {
      offs_by_label_[label_sorted] = v;
      cur_label = label_sorted;
    }

    // initialize core number
    core_num_[v] = adj_list[v].size();
    if (adj_list[v].size() > max_degree_) max_degree_ = adj_list[v].size();

    if (adj_list[v].size() == 0) {
      continue;
    }

    // sort by label first and degree second
    std::sort(adj_list[v].begin(), adj_list[v].end(),
              [this, &adj_list](Vertex v1, Vertex v2) -> bool {
                if (GetLabel(v1) != GetLabel(v2))
                  return GetLabel(v1) < GetLabel(v2);
                else
                  return adj_list[v1].size() > adj_list[v2].size();
              });

    // compute adj(v, l).start, adj(v, l).end for all l in nbr_label(v)
    Label cur_adj_label = GetLabel(adj_list[v][0]);
    adj_offs_by_label_[start + cur_adj_label].first = cur_idx;
    max_nbr_degree_[v] = adj_list[adj_list[v][0]].size();
    for (Size i = 1; i < adj_list[v].size(); ++i) {
      if (cur_adj_label != GetLabel(adj_list[v][i])) {
        linear_nbr_bitset_[nbr_bitset_size_ * v +
                           (cur_adj_label / (sizeof(uint64_t) * CHAR_BIT))] |=
            1ull << (cur_adj_label % (sizeof(uint64_t) * CHAR_BIT));
        if (max_nbr_degree_[v] < adj_list[adj_list[v][i]].size())
          max_nbr_degree_[v] = adj_list[adj_list[v][i]].size();
        adj_offs_by_label_[start + cur_adj_label].second = cur_idx + i;
        cur_adj_label = GetLabel(adj_list[v][i]);
        adj_offs_by_label_[start + cur_adj_label].first = cur_idx + i;
      }
    }
    linear_nbr_bitset_[nbr_bitset_size_ * v +
                       (cur_adj_label / (sizeof(uint64_t) * CHAR_BIT))] |=
        1ull << (cur_adj_label % (sizeof(uint64_t) * CHAR_BIT));

    adj_offs_by_label_[start + cur_adj_label].second =
        cur_idx + adj_list[v].size();

    // copy adj_list to linear_adj_list
    std::copy(adj_list[v].begin(), adj_list[v].end(),
              linear_adj_list_ + cur_idx);

    cur_idx += adj_list[v].size();
  }
  start_off_[GetNumVertices()] = num_edge_ * 2;
  offs_by_label_[GetNumLabels()] = GetNumVertices();

  // preprocess for data graph
  computeCoreNum();
}
}  // namespace daf
