#ifndef ENGINES_HQPS_APP_EXAMPLE_IC_IC_LABELS_H
#define ENGINES_HQPS_APP_EXAMPLE_IC_IC_LABELS_H

#include <string>

namespace gs {

/**Defines vertex and edges labels used in LDBC snb IC and IS Queries*/
struct SNBLabels {
  static std::string person_label;
  static std::string post_label;
  static std::string comment_label;

  static std::string place_label;

  static std::string org_label;

  static std::string knows_label;
  static std::string isLocatedIn_label;
  static std::string hasCreator_label;
  static std::string isPartOf_label;
  static std::string tag_label;

  static std::string container_of_label;
  static std::string forum_label;
  static std::string likes_label;
  static std::string hasMember_label;
  static std::string hasInterest_label;
  static std::string workAt_label;
  static std::string studyAt_label;
  static std::string replyOf_label;
  static std::string hasType_label;
  static std::string tagClass_label;
  static std::string is_subclass_of_label;
  static std::string hasTag_label;
  static std::string hasModerator_label;
};
std::string SNBLabels::person_label = "PERSON";
std::string SNBLabels::post_label = "POST";
std::string SNBLabels::comment_label = "COMMENT";
std::string SNBLabels::knows_label = "KNOWS";
std::string SNBLabels::isLocatedIn_label = "ISLOCATEDIN";
std::string SNBLabels::place_label = "PLACE";
std::string SNBLabels::workAt_label = "WORKAT";
std::string SNBLabels::studyAt_label = "STUDYAT";
std::string SNBLabels::org_label = "ORGANISATION";
std::string SNBLabels::hasCreator_label = "HASCREATOR";
std::string SNBLabels::isPartOf_label = "ISPARTOF";
std::string SNBLabels::tag_label = "TAG";
std::string SNBLabels::hasTag_label = "HASTAG";
std::string SNBLabels::container_of_label = "CONTAINEROF";
std::string SNBLabels::forum_label = "FORUM";
std::string SNBLabels::likes_label = "LIKES";
std::string SNBLabels::hasMember_label = "HASMEMBER";
std::string SNBLabels::hasInterest_label = "HASINTEREST";
std::string SNBLabels::replyOf_label = "REPLYOF";
std::string SNBLabels::hasType_label = "HASTYPE";
std::string SNBLabels::tagClass_label = "TAGCLASS";
std::string SNBLabels::is_subclass_of_label = "ISSUBCLASSOF";
std::string SNBLabels::hasModerator_label = "HASMODERATOR";

}  // namespace gs

#endif  // ENGINES_HQPS_APP_EXAMPLE_IC_IC_LABELS_H