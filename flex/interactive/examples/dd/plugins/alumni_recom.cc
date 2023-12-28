#include <queue>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

namespace gs {

static constexpr int32_t EMPLOYEE_CNT = 100000;

enum RecomReasonType {
  classMateAndColleague = 0,  // 同学兼同事
  exColleague = 1,            // 前同事
  commonFriend = 2,           // 共同好友
  commonGroup = 3,            // 共同群
  communication = 4,          // 最近沟通
  activeUser = 5,             // 活跃用户
  default = 6                 // 默认
};

class RecomReason {
  RecomReasonType type;
  int32_t num_common_group_or_friend;
  std::vector<vid_t> common_group_or_friend;
  int32_t org_id;  // 同事或者同学的组织id
};

// Recommend alumni for the input user.
class AlumniRecom : public AppBase {
 public:
  AlumniRecom(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        is_user_in_org_(false) {}

  bool Query(Decoder& input, Encoder& output) {
    users_in_org_ids_not_friend_.clear();
    is_user_in_org_ = false;

    int64_t user_id = input.get_long();
    int32_t page_id = input.get_int();
    int32_t limit = input.get_int();
    int32_t start_ind = (page_id) *limit;
    int32_t end_ind = (page_id + 1) * limit;
    auto txn = graph_.GetReadTransaction();

    vid_t vid;
    if (!txn.GetVertexIndex(user_label_id_, Any::From(user_id), vid)) {
      LOG(ERROR) << "Vertex not found: " << user_id;
      return false;
    }

    //首先通过亲密度边，拿到所有有亲密度的User
    auto user_user_view = txn.GetOutgoingGraphView<FixedChar>(
        user_label_id_, user_label_id_, intimacy_label_id_);
    auto user_org_view = txn.GetOutgoingGraphView<int64_t>(
        user_label_id_, ding_edu_org_label_id_, work_at_label_id_);
    // 首先去除掉所有好友

    std::vector<bool> valid_edu_org_ids;
    std::vector<vid_t> edu_org_ids;
    valid_edu_org_ids.resize(edu_org_num_, false);
    {
      for (auto& edge : org_view.GetEdges(vid)) {
        auto dst = edge.get_neighbor();
        valid_edu_org_ids[dst] = true;
        edu_org_ids.emplace_back(dst);
      }
    }

    std::vector<std::pair<vid_t, uint16_t>> intimacy_users;

    // only keep the user that are in the same org, and not friend
    traverse_intimacy_view(vid, user_user_view, user_org_view,
                           valid_edu_org_ids, edu_org_ids.intimacy_users);
    //

    VLOG(10) << "intimacy_users size: " << intimacy_users.size();
    if (start_ind > intimacy_users.size()) {
      start_ind = start_ind - intimacy_users.size();
      end_ind = end_ind - intimacy_users.size();
      std::vector<vid_t, RecomReason> res =
          try_without_intimacy(txn, vid, start_ind, end_ind);
      serialize(res, output);
      txn.Commit();
      return true;
    } else if (end_ind > intimacy_users.size()) {
      // In this case, the first part of recommend users are in initmacy users,
      // the second part are from try_with_intimacy
      sort_intimacy_users(intimacy_users);
      std::vector<vid_t, RecomReason> res =
          try_with_intimacy(txn, vid, 0, end_ind - intimacy_users.size());
      serialize(intimacy_users, res, output);
      txn.Commit();
      return true;
    } else {
      sort_intimacy_users(intimacy_users);
      serialize(intimacy_users, output);
      txn.Commit();
      return true;
    }
  }

  // 通过共同好友和共同群，拿到User,这里只保留和当前组织有过关系的用户
  void traverse_intimacy_view(vid vid,
                              const GraphView<FixedChar>& user_user_view,
                              const GraphView<int64_t>& user_org_view,
                              const std::vector<bool>& valid_org_ids,
                              const std::vector<vid_t> edu_org_ids,
                              std::vector<vid_t, uint16_t>& intimacy_users) {
    // 如果所有edu_org的领边加起来超过阈值，我们将通过用户来访问edu_org.
    // 如果所有edu_org的领边加起来不超过阈值，我们将通过edu_org来访问用户.
    size_t org_employee_cnt = 0;
    {
      for (auto org_id : edu_org_ids) {
        org_employee_cnt += user_org_view.GetEdges(org_id).estimated_degree();
      }
    }
    if (org_employee_cnt >= EMPLOYEE_CNT) {
      VLOG(10) << "traverse_intimacy_view: org_employee_cnt: "
               << org_employee_cnt << " >= " << EMPLOYEE_CNT;
      for (auto& edge : user_user_view.GetEdges(vid)) {
        auto dst = edge.get_neighbor();
        // only keep the user that are in the same org
        if (user_studied_at(dst, edu_org_ids)) {
          intimacy_users.emplace_back(dst);
          auto fc = edge.get_data();
          auto cur_ptr = static_cast<const char*>(fc.ptr);
          // the first it a uint8_t, the second is a uint16_t
          auto intimacy = *reinterpret_cast<const uint8_t*>(cur_ptr);
          auto comm_score = *reinterpret_cast<const uint16_t*>(cur_ptr + 1);
          intimacy_users.emplace_back(dst, intimacy + comm_score);
        }
      }
    } else {
      LOG(INFO) << "traverse_intimacy_view: org_employee_cnt: "
                << org_employee_cnt << " < " << EMPLOYEE_CNT;
      // In this case, we try to first calculate the bitset indicating whether a
      // person is in the orgs
      init_user_in_org_ids(user_org_view, edu_org_ids);

      for (auto& edge : user_user_view.GetEdges(vid)) {
        auto dst = edge.get_neighbor();
        if (users_in_org_ids_not_friend_.count(dst) > 0) {
          auto fc = edge.get_data();
          auto cur_ptr = static_cast<const char*>(fc.ptr);
          // the first it a uint8_t, the second is a uint16_t
          auto intimacy = *reinterpret_cast<const uint8_t*>(cur_ptr);
          auto comm_score = *reinterpret_cast<const uint16_t*>(cur_ptr + 1);
          intimacy_users.emplace_back(dst, intimacy + comm_score);
        }
      }
    }
  }

  void init_user_in_org_ids(const GraphView<int64_t>& user_org_view,
                            const std::vector<vid_t>& edu_org_ids) {
    CHECK(!is_user_in_org_);
    for (auto org_id : edu_org_ids) {
      for (auto& edge : user_org_view.GetEdges(org_id)) {
        auto dst = edge.get_neighbor();
        users_in_org_ids_not_friend_.set_bit(dst);
      }
    }
    is_user_in_org_ = true;
  }

  // Find the recommend users located in [start_ind, end_ind) from the result
  // with intimacy
  void try_without_intimacy(const ReadTransaction& txn, vid_t vid,
                            int32_t start_ind, int32_t end_ind,
                            std::vector<vid_t, RecomReason>& res) {
    auto user_user_view = txn.GetOutgoingGraphView<FixedChar>(
        user_label_id_, user_label_id_, intimacy_label_id_);
    auto user_org_view = txn.GetOutgoingGraphView<int64_t>(
        user_label_id_, ding_edu_org_label_id_, work_at_label_id_);
    auto user_group_view = txn.GetOutgoingGraphView<int64_t>(
        user_label_id_, ding_group_label_id_, chat_in_group_label_id_);
    auto user_friend_view = txn.GetOutgoingGraphView<int64_t>(
        user_label_id_, user_label_id_, friend_label_id_);

    std::vector<bool> valid_edu_org_ids;
    std::vector<vid_t> edu_org_ids;
    valid_edu_org_ids.resize(edu_org_num_, false);
    {
      for (auto& edge : org_view.GetEdges(vid)) {
        auto dst = edge.get_neighbor();
        valid_edu_org_ids[dst] = true;
        edu_org_ids.emplace_back(dst);
      }
    }

    std::vector<vid_t> common_group_users;
    std::vector<vid_t> common_friend_users;
    std::vector<vid_t> ex_colleague_users;
    std::vector<vid_t> classmate_and_colleague_users;
    std::vector<vid_t> communication_users;
    std::vector<vid_t> active_users;

    traverse_group_view(user_group_view, valid_edu_org_ids, edu_org_ids,
                        common_group_users);
    traverse_friend_view(user_friend_view, valid_edu_org_ids, edu_org_ids,
                         common_friend_users);
    traverse_org_view(user_org_view, valid_edu_org_ids, edu_org_ids,
                      ex_colleague_users, classmate_and_colleague_users);
    traverse_communication_view(user_user_view, valid_edu_org_ids, edu_org_ids,
                                communication_users);
    traverse_active_view(user_user_view, valid_edu_org_ids, edu_org_ids,
                         active_users);

    std::vector<std::pair<vid_t, uint16_t>> res;
    res.reserve(common_group_users.size() + common_friend_users.size() +
                ex_colleague_users.size() +
                classmate_and_colleague_users.size() +
                communication_users.size() + active_users.size());
  }

 private:
  GraphDBSession& graph_;
  label_t user_label_id_;
  label_t ding_org_label_id_;
  label_t ding_edu_org_label_id_;
  label_t ding_group_label_id_;

  label_t friend_label_id_;
  label_t chat_in_group_label_id_;
  label_t intimacy_label_id_;
  label_t work_at_label_id_;
  label_t study_at_label_id_;

  int32_t edu_org_num_ = 0;
  int32_t users_num_ = 0;

  bool is_user_in_org_ = false;

  std::unordered_set<vid_t> users_in_org_ids_not_friend_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::AlumniRecom* app = new gs::AlumniRecom(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::AlumniRecom* casted = static_cast<gs::AlumniRecom*>(app);
  delete casted;
}
}