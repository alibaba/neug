#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

std::vector<std::string> split(const std::string& s, char delim) {
  std::vector<std::string> elems;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(std::move(item));
  }
  return elems;
}

int main(int argc, char** argv) {
  if (argc < 4) {
    printf("Usage: %s <message_table> <creator_table> <output_file>\n",
           argv[0]);
    exit(1);
  }
  std::string message_table = argv[1];
  std::string creator_table = argv[2];
  std::string output_file = argv[3];

  std::ifstream fin1(message_table);
  std::ifstream fin2(creator_table);

  std::string header1, header2;

  std::getline(fin1, header1);
  std::getline(fin2, header2);

  std::vector<std::string> header1s = split(header1, '|');
  std::vector<std::string> header2s = split(header2, '|');

  if (header2s.size() != 2) {
    std::cerr << "Creator file - " << creator_table << " - has "
              << header2s.size() << " columns, not 2\n";
    return 1;
  }
  int creationDate_idx = -1;
  for (int i = 0; i < header1s.size(); ++i) {
    if (header1s[i] == "creationDate") {
      creationDate_idx = i;
      break;
    }
  }
  if (creationDate_idx == -1) {
    std::cerr << "Message file - " << message_table
              << " - does not have a creationDate column\n";
    return 1;
  }

  std::ofstream fout(output_file);
  fout << header2 << "|" << header1s[creationDate_idx] << "\n";

  while (!fin1.eof() && !fin2.eof()) {
    std::string line1, line2;
    std::getline(fin1, line1);
    std::getline(fin2, line2);

    if (line1.empty() || line2.empty()) {
      break;
    }

    std::vector<std::string> line1s = split(line1, '|');
    std::vector<std::string> line2s = split(line2, '|');

    if (line1s[0] != line2s[0]) {
      std::cerr << "Mismatched ids: " << line1s[0] << " != " << line2s[0]
                << "\n";
      return 1;
    }

    std::string creationDate_value = line1s[creationDate_idx];
    fout << line2 << "|" << creationDate_value << "\n";
  }

  fout.flush();
  fout.close();

  fin1.close();
  fin2.close();

  return 0;
}