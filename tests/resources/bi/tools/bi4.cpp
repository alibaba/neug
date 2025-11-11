#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

void load_vertex(const std::string& path,
                 std::unordered_map<int64_t, uint32_t>& vertex_map,
                 bool skip_first_col = true) {
  std::ifstream file(path);
  std::string line;
  uint32_t ind = 0;
  std::getline(file, line);  // skip header
  while (std::getline(file, line)) {
    std::istringstream ss(line);
    std::string token;
    if (skip_first_col) {
      std::getline(ss, token, '|');
    }
    std::getline(ss, token, '|');
    int64_t vertex_id = std::stoll(token);
    vertex_map[vertex_id] = ind++;
  }
  file.close();
}

void load_edge(const std::string& path,
               const std::unordered_map<int64_t, uint32_t>& src_map,
               const std::unordered_map<int64_t, uint32_t>& dst_map,
               std::vector<std::vector<uint32_t>>& edges, bool dir,
               bool skip_first_col = true) {
  std::ifstream file(path);
  std::string line;
  std::getline(file, line);  // skip header
  edges.resize(src_map.size());
  while (std::getline(file, line)) {
    std::istringstream ss(line);
    std::string token;
    if (skip_first_col) {
      std::getline(ss, token, '|');
    }
    std::getline(ss, token, '|');
    int64_t src_id = std::stoll(token);
    std::getline(ss, token, '|');
    int64_t dst_id = std::stoll(token);
    auto src_vid = src_map.at(dir ? src_id : dst_id);
    auto dst_vid = dst_map.at(dir ? dst_id : src_id);

    edges[src_vid].push_back(dst_vid);
  }
  file.close();
}
int main(int argc, char** argv) {
  const char* dir_path = argv[1];
  std::string place = std::string(dir_path) + "/static/Place/merged.csv";
  std::string person = std::string(dir_path) + "/dynamic/Person/merged.csv";
  std::string forum = std::string(dir_path) + "/dynamic/Forum/merged.csv";
  std::string place_isPartOf_place =
      std::string(dir_path) + "/static/Place_isPartOf_Place/merged.csv";
  std::string comment_hascreator_person =
      std::string(dir_path) + "/dynamic/Comment_hasCreator_Person/merged.csv";
  std::string person_isLocatedIn_place =
      std::string(dir_path) + "/dynamic/Person_isLocatedIn_Place/merged.csv";
  std::string forum_hasMember_person =
      std::string(dir_path) + "/dynamic/Forum_hasMember_Person/merged.csv";

  std::unordered_map<int64_t, uint32_t> person_map, place_map, forum_map;
  load_vertex(person, person_map);
  load_vertex(place, place_map, false);
  load_vertex(forum, forum_map);
  std::vector<std::vector<uint32_t>> place_isPartOf_place_edges;
  load_edge(place_isPartOf_place, place_map, place_map,
            place_isPartOf_place_edges, true, false);
  std::vector<std::vector<uint32_t>> forum_hasMember_person_edges;
  load_edge(forum_hasMember_person, forum_map, person_map,
            forum_hasMember_person_edges, true);
  std::vector<std::vector<uint32_t>> person_isLocatedIn_place_edges;
  load_edge(person_isLocatedIn_place, person_map, place_map,
            person_isLocatedIn_place_edges, true);
  std::unordered_map<uint32_t, int64_t> rev_forum_map;
  for (const auto& kv : forum_map) {
    rev_forum_map[kv.second] = kv.first;
  }
  std::map<int64_t, size_t> count_map;
  for (size_t i = 0; i < forum_map.size(); ++i) {
    std::unordered_map<uint32_t, size_t> loc_count;
    for (auto person_vid : forum_hasMember_person_edges[i]) {
      for (auto place_vid : person_isLocatedIn_place_edges[person_vid]) {
        for (auto parent_place_vid : place_isPartOf_place_edges[place_vid]) {
          loc_count[parent_place_vid]++;
        }
      }
    }
    size_t popularity = 0;
    for (const auto& kv : loc_count) {
      popularity = std::max(popularity, kv.second);
    }
    count_map[rev_forum_map[i]] = popularity;
  }

  std::ifstream forum_file(forum);
  std::string output =
      std::string(dir_path) + "/dynamic/Forum/merged_with_counts-1.csv";
  std::ofstream output_file(output);
  std::string line;
  std::getline(forum_file, line);
  output_file << line << "|popularity\n";
  while (std::getline(forum_file, line)) {
    std::istringstream ss(line);
    std::string token;
    std::getline(ss, token, '|');
    std::getline(ss, token, '|');
    int64_t person_id = std::stoll(token);
    output_file << line << "|" << count_map[person_id] << "\n";
  }
}