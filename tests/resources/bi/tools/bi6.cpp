#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

void load_vertex(const std::string& path,
                 std::unordered_map<int64_t, uint32_t>& vertex_map) {
  std::ifstream file(path);
  std::string line;
  uint32_t ind = 0;
  std::getline(file, line);  // skip header
  while (std::getline(file, line)) {
    std::istringstream ss(line);
    std::string token;
    std::getline(ss, token, '|');
    std::getline(ss, token, '|');
    int64_t vertex_id = std::stoll(token);
    vertex_map[vertex_id] = ind++;
  }
  file.close();
}

void load_edge(const std::string& path,
               const std::unordered_map<int64_t, uint32_t>& src_map,
               const std::unordered_map<int64_t, uint32_t>& dst_map,
               std::vector<std::vector<uint32_t>>& edges, bool dir) {
  std::ifstream file(path);
  std::string line;
  std::getline(file, line);  // skip header
  edges.resize(src_map.size());
  while (std::getline(file, line)) {
    std::istringstream ss(line);
    std::string token;
    std::getline(ss, token, '|');
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
  std::string person = std::string(dir_path) + "/dynamic/Person/merged.csv";
  std::string post = std::string(dir_path) + "/dynamic/Post/merged.csv";
  std::string comment = std::string(dir_path) + "/dynamic/Comment/merged.csv";
  std::string post_hascreator_person =
      std::string(dir_path) + "/dynamic/Post_hasCreator_Person/merged.csv";
  std::string comment_hascreator_person =
      std::string(dir_path) + "/dynamic/Comment_hasCreator_Person/merged.csv";
  std::string person_likes_post =
      std::string(dir_path) + "/dynamic/Person_likes_Post/merged.csv";
  std::string person_likes_comment =
      std::string(dir_path) + "/dynamic/Person_likes_Comment/merged.csv";
  std::unordered_map<int64_t, uint32_t> person_map, post_map, comment_map;
  load_vertex(person, person_map);
  load_vertex(post, post_map);
  load_vertex(comment, comment_map);
  std::vector<std::vector<uint32_t>> post_creator_edges;
  load_edge(post_hascreator_person, person_map, post_map, post_creator_edges,
            false);
  std::vector<std::vector<uint32_t>> comment_creator_edges;
  load_edge(comment_hascreator_person, person_map, comment_map,
            comment_creator_edges, false);
  std::vector<std::vector<uint32_t>> person_likes_post_edges;
  load_edge(person_likes_post, post_map, person_map, person_likes_post_edges,
            false);
  std::vector<std::vector<uint32_t>> person_likes_comment_edges;
  load_edge(person_likes_comment, comment_map, person_map,
            person_likes_comment_edges, false);
  std::unordered_map<int64_t, uint32_t> count_map;
  std::unordered_map<uint32_t, int64_t> reverse_person_map;
  for (const auto& pair : person_map) {
    reverse_person_map[pair.second] = pair.first;
  }
  for (size_t i = 0; i < person_map.size(); ++i) {
    for (auto post_id : post_creator_edges[i]) {
      for (auto liker_id : person_likes_post_edges[post_id]) {
        count_map[reverse_person_map[i]]++;
      }
    }
    for (auto comment_id : comment_creator_edges[i]) {
      for (auto liker_id : person_likes_comment_edges[comment_id]) {
        count_map[reverse_person_map[i]]++;
      }
    }
  }
  std::ifstream person_file(person);
  std::string output =
      std::string(dir_path) + "/dynamic/Person/merged_with_counts.csv";
  std::ofstream output_file(output);
  std::string line;
  std::getline(person_file, line);
  output_file << line << "|popularityScore\n";
  while (std::getline(person_file, line)) {
    std::istringstream ss(line);
    std::string token;
    std::getline(ss, token, '|');
    std::getline(ss, token, '|');
    int64_t person_id = std::stoll(token);
    output_file << line << "|" << count_map[person_id] << "\n";
  }
}