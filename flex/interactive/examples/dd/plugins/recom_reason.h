#ifndef FLEX_INTERACTIVE_EXAMPLES_DD_PLUGINS_RECOM_REASON_H_

#include <string>
#include <vector>
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

enum RecomReasonType {
  kExColleague = 0,    // 前同事
  kCurColleague = 1,   // 当前同事
  kCommonFriend = 2,   // 共同好友
  kCommonGroup = 3,    // 共同群
  kCommunication = 4,  // 最近沟通
  kActiveUser = 5,     // 活跃用户
  kAlumni = 6,         // 校友
  kDefault = 7         // 默认
};

struct RecomReason {
  RecomReason() : type(kDefault) {}

  static RecomReason Default() {
    RecomReason reason;
    reason.type = kDefault;
    return reason;
  }

  static RecomReason Active() {
    RecomReason reason;
    reason.type = kActiveUser;
    return reason;
  }

  static RecomReason Colleague() {
    RecomReason reason;
    reason.type = kCurColleague;
    return reason;
  }

  static RecomReason Alumni() {
    RecomReason reason;
    reason.type = kAlumni;
    return reason;
  }

  static RecomReason CommonFriend(vid_t first, vid_t second) {
    RecomReason reason;
    reason.type = kCommonFriend;
    if (first != INVALID_VID) {
      reason.common_group_or_friend.push_back(first);
    }
    if (second != INVALID_VID) {
      reason.common_group_or_friend.push_back(second);
    }
    return reason;
  }

  static RecomReason CommonGroup(vid_t first, vid_t second) {
    RecomReason reason;
    reason.type = kCommonGroup;
    if (first != INVALID_VID) {
      reason.common_group_or_friend.push_back(first);
    }
    if (second != INVALID_VID) {
      reason.common_group_or_friend.push_back(second);
    }
    return reason;
  }

  std::string ToJsonString() const {
    std::string json_str = "{\"type\":" + std::to_string(type);
    if (type == kCommonFriend || type == kCommonGroup) {
      json_str += ",\"resourceList\":[";
      for (size_t i = 0; i < common_group_or_friend.size(); ++i) {
        json_str += std::to_string(common_group_or_friend[i]);
        if (i != common_group_or_friend.size() - 1) {
          json_str += ",";
        }
      }
      json_str += "]";
      // add resource num
      json_str +=
          ",\"resourceNum\":" + std::to_string(common_group_or_friend.size());
    }
    json_str += "}";
    VLOG(10) << "recom reason json: " << json_str;
    return json_str;
  }

  RecomReasonType type;
  std::vector<vid_t> common_group_or_friend;
  int32_t org_id;  // 同事或者同学的组织id
};

void get_friends(
    std::vector<vid_t>& friends, std::unordered_set<vid_t>& visited,
    const ImmutableAdjListView<grape::EmptyType>& friend_edges_oe,
    const ImmutableAdjListView<grape::EmptyType>& friend_edges_ie) {
  // 1-hop friends
  for (auto& e : friend_edges_oe) {
    friends.emplace_back(e.get_neighbor());
    visited.emplace(e.get_neighbor());
  }
  for (auto& e : friend_edges_ie) {
    friends.emplace_back(e.get_neighbor());
    visited.emplace(e.get_neighbor());
  }
  std::sort(friends.begin(), friends.end());
  size_t friend_num =
      std::unique(friends.begin(), friends.end()) - friends.begin();
  friends.resize(friend_num);
  return;
}

void serialize_result(GraphDBSession& graph, Encoder& output,
                      label_t user_label_id,
                      const std::vector<vid_t>& return_vec,
                      const std::vector<RecomReason>& return_reasons) {
  CHECK(return_vec.size() == return_reasons.size());
  output.put_int(return_vec.size());
  for (auto i = 0; i < return_vec.size(); ++i) {
    output.put_long(
        graph.graph().get_oid(user_label_id, return_vec[i]).AsInt64());
    output.put_string(return_reasons[i].ToJsonString());
  }
}

}  // namespace gs

#endif  // FLEX_INTERACTIVE_EXAMPLES_DD_PLUGINS_RECOM_REASON_H_
