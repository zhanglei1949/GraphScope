#ifndef GRAPHSCOPE_OPERATOR_PROP_UTILS_H_
#define GRAPHSCOPE_OPERATOR_PROP_UTILS_H_

#include <array>
#include <string>
#include <tuple>
#include <vector>

namespace gs {

// forward declare context
template <typename HEAD_T, int cur_alias, int base_tag, typename... ALIAS_SETS>
class Context;

// forward declare flat_edge_set
template <typename VID_T, typename LabelT, size_t N, typename... T>
class FlatEdgeSet;

// forward declare general_edge_set
template <size_t N, typename GI, typename VID_T, typename LabelT, typename... T>
class GeneralEdgeSet;

// forward declare keyed_row_vertex_set
template <typename LabelT, typename KEY_T, typename VID_T, typename... T>
class KeyedRowVertexSetImpl;

// forward declare row_vertex_set
template <typename LabelT, typename VID_T, typename... T>
class RowVertexSetImpl;

// forward declare two_label_vertex_set
template <typename VID_T, typename LabelT, typename... T>
class TwoLabelVertexSetImpl;

template <typename GI, typename PropTupleT>
struct MultiPropGetterT;

template <typename GI, typename... T>
struct MultiPropGetterT<GI, std::tuple<T...>> {};

template <typename T>
class Collection;

template <typename GRAPH_INTERFACE, typename LabelT, typename... T>
static auto get_prop_getter_from_named_property(
    const GRAPH_INTERFACE& graph, const LabelT& label,
    const std::tuple<NamedProperty<T>...>& named_property) {
  std::array<std::string, sizeof...(T)> prop_names;
  int i = 0;
  std::apply(
      [&prop_names, &i](auto&... named_prop) {
        ((prop_names[i++] = named_prop.name), ...);
      },
      named_property);
  // LOG(INFO) << "get multiple prop getters";
  return graph.template GetMultiPropGetter<T...>(label, prop_names);
}

//// Get one property getter for one label
template <typename GRAPH_INTERFACE, typename LabelT, typename... PropT>
static auto get_prop_getter_from_selectors(
    const GRAPH_INTERFACE& graph, const LabelT& label,
    const std::tuple<PropertySelector<PropT>...>& selectors) {
  std::array<std::string, sizeof...(PropT)> prop_names;
  int i = 0;
  std::apply(
      [&prop_names, &i](auto&... named_prop) {
        ((prop_names[i++] = named_prop.prop_name_), ...);
      },
      selectors);
  // LOG(INFO) << "get multiple prop getters";
  return graph.template GetMultiPropGetter<PropT...>(label, prop_names);
}
/// get single property getter for one label
template <typename GRAPH_INTERFACE, typename LabelT, typename PropT>
static auto get_single_prop_getter_from_selector(
    const GRAPH_INTERFACE& graph, const LabelT& label,
    const PropertySelector<PropT>& selector) {
  auto prop_name = selector.prop_name_;
  // LOG(INFO) << "get multiple prop getters";
  return graph.template GetSinglePropGetter<PropT>(label, prop_name);
}

// get prop getter from multiplet named property
template <typename GRAPH_INTERFACE, typename LabelT, typename... NamedPropT>
static auto get_prop_getters_from_named_property(
    const GRAPH_INTERFACE& graph, const LabelT& label,
    std::tuple<NamedPropT...> named_property) {
  std::array<LabelT, 1> labels = {label};
  return get_prop_getters_from_named_property(graph, labels, named_property);
}

template <typename GRAPH_INTERFACE, typename LabelT, size_t num_labels,
          typename... NamedPropT, size_t... Is>
static auto get_prop_getters_from_named_property(
    const GRAPH_INTERFACE& graph, const std::array<LabelT, num_labels>& labels,
    std::tuple<NamedPropT...> named_property, std::index_sequence<Is...>) {
  // LOG(INFO) << "get_prop_getters_from_named_property, for labels"
  //           << gs::to_string(labels);
  using prop_getter_t = typename GRAPH_INTERFACE::template multi_prop_getter_t<
      typename NamedPropT::prop_t...>;
  std::array<prop_getter_t, num_labels> prop_getter_array{
      get_prop_getter_from_named_property(graph, labels[Is],
                                          named_property)...};
  return prop_getter_array;
}

// Get prop getters from Selector.
template <typename GRAPH_INTERFACE, typename LabelT, size_t num_labels,
          typename... SELECTOR, size_t... Is>
static auto get_prop_getters_from_selectors_impl(
    const GRAPH_INTERFACE& graph, const std::array<LabelT, num_labels>& labels,
    std::tuple<SELECTOR...> selectors, std::index_sequence<Is...>) {
  // LOG(INFO) << "get_prop_getters_from_named_property, for labels"
  //           << gs::to_string(labels);
  using prop_getter_t = typename GRAPH_INTERFACE::template multi_prop_getter_t<
      typename SELECTOR::prop_t...>;
  std::array<prop_getter_t, num_labels> prop_getter_array{
      get_prop_getter_from_selectors(graph, labels[Is], selectors)...};
  return prop_getter_array;
}

template <typename GRAPH_INTERFACE, typename LabelT, size_t num_labels,
          typename... SELECTOR>
static auto get_prop_getters_from_selectors(
    const GRAPH_INTERFACE& graph, const std::array<LabelT, num_labels>& labels,
    std::tuple<SELECTOR...> named_property) {
  return get_prop_getters_from_selectors_impl(
      graph, labels, named_property, std::make_index_sequence<num_labels>{});
}

///////////////////////// prop getter for vertex set
//////////////////////////////

template <int tag_id, typename VID_T>
class InnerIdGetter {
 public:
  InnerIdGetter(const std::vector<VID_T>& vids) : vids_(vids) {}

  VID_T get_view(const std::tuple<size_t, VID_T>& ele) const {
    return std::get<1>(ele);
  }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    return gs::get_from_tuple<tag_id>(all_ele);
  }

 private:
  const std::vector<VID_T>& vids_;
};

template <int tag_id, typename VID_T, typename... EDATA_T>
class EdgeSetInnerIdGetter {
 public:
  EdgeSetInnerIdGetter() {}

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    return gs::get_from_tuple<tag_id>(all_ele);
  }
};

template <int tag_id, typename T>
class CollectionPropGetter {
 public:
  CollectionPropGetter() {}

  inline auto get_view(const std::tuple<size_t, T>& ele) const {
    return std::get<1>(ele);
  }

  inline auto get_view() const { return std::get<1>(ind_ele_); }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    return gs::get_from_tuple<tag_id>(all_ele);
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = ind_ele;
  }

 private:
  std::tuple<size_t, T> ind_ele_;
};

// specialize for collection with only one column
template <int tag_id, typename T>
class CollectionPropGetter<tag_id, std::tuple<T>> {
 public:
  CollectionPropGetter() {}

  inline T get_view(const std::tuple<size_t, T>& ele) const {
    return std::get<1>(ele);
  }

  inline T get_view() const { return std::get<1>(ind_ele_); }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    return gs::get_from_tuple<tag_id>(all_ele);
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = ind_ele;
  }

 private:
  std::tuple<size_t, T> ind_ele_;
};

template <int tag_id, typename index_ele_tuple_t>
class FlatEdgeSetPropGetter {
 public:
  FlatEdgeSetPropGetter() {}

  inline auto get_view(const index_ele_tuple_t& ind_ele) const {
    return std::get<0>(std::get<2>(std::get<1>(ind_ele)));
  }

  inline auto get_view()
      const {  // const std::tuple<size_t, int32_t, VID_T>& ind_ele
    return std::get<0>(std::get<2>(std::get<1>(ind_ele_)));
  }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    auto& my_ele = gs::get_from_tuple<tag_id>(all_ele);
    return std::get<0>(std::get<2>(my_ele));
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  index_ele_tuple_t ind_ele_;
};

template <int tag_id, typename index_ele_tuple_t>
class GeneralEdgeSetPropGetter {
 public:
  GeneralEdgeSetPropGetter() {}

  inline auto get_view(const index_ele_tuple_t& ind_ele) const {
    return std::get<0>(std::get<2>(ind_ele).properties());
  }

  inline auto get_view()
      const {  // const std::tuple<size_t, int32_t, VID_T>& ind_ele
    return std::get<0>(std::get<2>(ind_ele_).properties());
  }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    auto& my_ele = gs::get_from_tuple<tag_id>(all_ele);
    return std::get<0>(std::get<1>(my_ele).properties());
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  index_ele_tuple_t ind_ele_;
};

template <int tag_id, typename PROP_GETTER_T, typename IND_ELE_T>
class TwoLabelVertexSetImplPropGetter {
 public:
  TwoLabelVertexSetImplPropGetter(std::array<PROP_GETTER_T, 2>&& getters)
      : getters_(std::move(getters)) {}

  inline auto get_view(const IND_ELE_T& ind_ele) const {
    return getters_[std::get<1>(ind_ele)].get_view(std::get<2>(ind_ele));
  }

  inline auto get_view()
      const {  // const std::tuple<size_t, int32_t, VID_T>& ind_ele
    return getters_[std::get<1>(ind_ele_)].get_view(std::get<2>(ind_ele_));
  }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    auto& my_ele = gs::get_from_tuple<tag_id>(all_ele);
    auto& getter = getters_[std::get<0>(my_ele)];
    return getter.get_view(std::get<1>(my_ele));
  }

  template <typename ELE_T>
  inline auto get_from_element(const ELE_T& ele) const {
    return getters_[std::get<0>(ele)].get_view(std::get<1>(ele));
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  IND_ELE_T ind_ele_;
  std::array<PROP_GETTER_T, 2> getters_;
};

template <int tag_id, typename PROP_GETTER_T, typename IND_ELE_T>
class RowVertexSetPropGetter {
 public:
  RowVertexSetPropGetter(PROP_GETTER_T&& getter) : getter_(std::move(getter)) {}

  template <typename VID_T>
  inline auto get_view(const std::tuple<size_t, VID_T>& ind_ele) const {
    return getter_.get_view(std::get<1>(ind_ele));
  }

  inline auto get_view() const {
    return getter_.get_view(std::get<1>(ind_ele_));
  }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    auto& my_ele = gs::get_from_tuple<tag_id>(all_ele);
    return getter_.get_view(my_ele);
  }

  // get from ele
  template <typename ELE_T>
  inline auto get_from_element(const ELE_T& ele) const {
    return getter_.get_view(ele);
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  IND_ELE_T ind_ele_;
  PROP_GETTER_T getter_;
};

template <int tag_id, typename PROP_GETTER_T, typename IND_ELE_T>
class KeyedRowVertexSetPropGetter {
 public:
  KeyedRowVertexSetPropGetter(PROP_GETTER_T&& getter)
      : getter_(std::move(getter)) {}

  template <typename VID_T>
  inline auto get_view(const std::tuple<size_t, VID_T>& ind_ele) const {
    return getter_.get_view(std::get<1>(ind_ele));
  }

  inline auto get_view() const {
    return getter_.get_view(std::get<1>(ind_ele_));
  }

  template <typename ALL_ELE_T>
  inline auto get_from_all_element(const ALL_ELE_T& all_ele) const {
    auto& my_ele = gs::get_from_tuple<tag_id>(all_ele);
    return getter_.get_view(my_ele);
  }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  IND_ELE_T ind_ele_;
  PROP_GETTER_T getter_;
};

template <int tag_id, typename IND_ELE_T>
class DistGetter {
 public:
  DistGetter(std::vector<Dist>&& dist) : dist_(std::move(dist)) {}

  template <typename VID_T>
  inline auto get_view(const std::tuple<size_t, VID_T>& ind_ele) const {
    return dist_[std::get<0>(ind_ele)];
  }

  inline auto get_view() const { return dist_[std::get<0>(ind_ele_)]; }

  template <typename ALL_IND_ELE_T>
  inline void set_ind_ele(const ALL_IND_ELE_T& ind_ele) {
    ind_ele_ = gs::get_from_tuple<tag_id>(ind_ele);
  }

 private:
  std::vector<Dist> dist_;
  IND_ELE_T ind_ele_;
};

///////////////////////Creating property getter/////////////////////////

template <int tag_id, size_t Is = 0, typename LabelT, typename VID_T,
          typename... T>
static auto get_dist_prop_getter(
    const RowVertexSetImpl<LabelT, VID_T, T...>& set,
    const std::array<std::string, sizeof...(T)>& prop_names) {
  if (prop_names[Is] == "dist" || prop_names[Is] == "Dist") {
    std::vector<Dist> dists;
    auto& data_vec = set.GetDataVec();
    dists.reserve(set.Size());
    for (auto i = 0; i < data_vec.size(); ++i) {
      dists.emplace_back(Dist(std::get<Is>(data_vec[i])));
    }
    return DistGetter<tag_id, typename RowVertexSetImpl<
                                  LabelT, VID_T, T...>::index_ele_tuple_t>(
        std::move(dists));
  }
  if constexpr (Is + 1 >= sizeof...(T)) {
    LOG(WARNING) << "Property dist not found, using default 0";
    std::vector<Dist> dists;
    auto set_size = set.Size();
    dists.reserve(set_size);
    for (auto i = 0; i < set_size; ++i) {
      dists.emplace_back(0);
    }
    return DistGetter<tag_id, typename RowVertexSetImpl<
                                  LabelT, VID_T, T...>::index_ele_tuple_t>(
        std::move(dists));
  } else {
    return get_dist_prop_getter<tag_id, Is + 1>(set, prop_names);
  }
}

// get for common properties for rwo_vertex_set
template <
    int tag_id, typename prop_t, typename GRAPH_INTERFACE, typename LabelT,
    typename VID_T, typename... T,
    typename std::enable_if<!std::is_same_v<prop_t, Dist>>::type* = nullptr>
static auto create_prop_getter_impl(
    const RowVertexSetImpl<LabelT, VID_T, T...>& set,
    const GRAPH_INTERFACE& graph, const std::string& prop_name) {
  using prop_getter_t =
      typename GRAPH_INTERFACE::template single_prop_getter_t<prop_t>;
  // const std::array<std::string, 2>& labels = set.GetLabels();

  auto label = set.GetLabel();
  LOG(INFO) << "getting getter for " << prop_name << " for label "
            << gs::to_string(label);
  auto getter = graph.template GetSinglePropGetter<prop_t>(label, prop_name);
  return RowVertexSetPropGetter<
      tag_id, prop_getter_t,
      typename RowVertexSetImpl<LabelT, VID_T, T...>::index_ele_tuple_t>(
      std::move(getter));
}

// get for dist property for row_vertex_set
template <
    int tag_id, typename prop_t, typename GRAPH_INTERFACE, typename LabelT,
    typename VID_T, typename... T,
    typename std::enable_if<std::is_same_v<prop_t, Dist>>::type* = nullptr>
static auto create_prop_getter_impl(
    const RowVertexSetImpl<LabelT, VID_T, T...>& set,
    const GRAPH_INTERFACE& graph, const std::string& prop_name) {
  LOG(INFO) << "Getting dist prop getter";
  CHECK(prop_name == "dist" || prop_name == "Dist");
  return get_dist_prop_getter<tag_id>(set, set.GetPropNames());
}

// get for common properties for two_label_vertex_set
template <int tag_id, typename prop_t, typename GRAPH_INTERFACE, typename VID_T,
          typename LabelT, typename... T>
static auto create_prop_getter_impl(
    const TwoLabelVertexSetImpl<VID_T, LabelT, T...>& set,
    const GRAPH_INTERFACE& graph, const std::string& prop_name) {
  using prop_getter_t =
      typename GRAPH_INTERFACE::template single_prop_getter_t<prop_t>;
  auto& labels = set.GetLabels();
  std::array<std::string, 1> names{prop_name};
  LOG(INFO) << "Getting prop labels for " << prop_name << " for labels "
            << std::to_string(labels[0]) << ", " << std::to_string(labels[1]);
  std::array<prop_getter_t, 2> prop_getter{
      graph.template GetSinglePropGetter<prop_t>(labels[0], prop_name),
      graph.template GetSinglePropGetter<prop_t>(labels[1], prop_name)};

  return TwoLabelVertexSetImplPropGetter<
      tag_id, prop_getter_t,
      typename TwoLabelVertexSetImpl<VID_T, LabelT, T...>::index_ele_tuple_t>(
      std::move(prop_getter));
}

// get for common properties for keyed_row_vertex_set
template <int tag_id, typename prop_t, typename GRAPH_INTERFACE,
          typename LabelT, typename KEY_T, typename VID_T, typename... T>
static auto create_prop_getter_impl(
    const KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T, T...>& set,
    const GRAPH_INTERFACE& graph, const std::string& prop_name) {
  using prop_getter_t =
      typename GRAPH_INTERFACE::template single_prop_getter_t<prop_t>;
  // const std::array<std::string, 2>& labels = set.GetLabels();
  auto label = set.GetLabel();

  auto getter = graph.template GetSinglePropGetter<prop_t>(label, prop_name);
  return KeyedRowVertexSetPropGetter<
      tag_id, prop_getter_t,
      typename KeyedRowVertexSetImpl<LabelT, KEY_T, VID_T,
                                     T...>::index_ele_tuple_t>(
      std::move(getter));
}

// get for common properties for FlatEdgeSet
template <int tag_id, typename prop_t, typename GRAPH_INTERFACE, typename VID_T,
          typename LabelT, size_t N, typename... EDATA_T>
static auto create_prop_getter_impl(
    const FlatEdgeSet<VID_T, LabelT, N, EDATA_T...>& set,
    const GRAPH_INTERFACE& graph, const std::string& prop_name) {
  return FlatEdgeSetPropGetter<
      tag_id,
      typename FlatEdgeSet<VID_T, LabelT, N, EDATA_T...>::index_ele_tuple_t>();
}

// get for common properties for GeneralEdgeSet
template <int tag_id, typename prop_t, size_t N, typename GI, typename VID_T,
          typename LabelT, typename... EDATA_T>
static auto create_prop_getter_impl(
    const GeneralEdgeSet<N, GI, VID_T, LabelT, EDATA_T...>& set,
    const GI& graph, const std::string& prop_name) {
  return GeneralEdgeSetPropGetter<
      tag_id, typename GeneralEdgeSet<N, GI, VID_T, LabelT,
                                      EDATA_T...>::index_ele_tuple_t>();
}

// get for common properties for collection
template <int tag_id, typename prop_t, typename GI, typename T>
static auto create_prop_getter_impl(const Collection<T>& set, const GI& graph,
                                    const std::string& prop_name) {
  CHECK(prop_name == "None" || prop_name == "none");
  return CollectionPropGetter<tag_id, T>();
}

// create for inner id getter.
template <typename GRAPH_INTERFACE, typename LabelT, typename... SET_Ts,
          int tag_id>
static auto create_prop_getter_from_prop_desc(
    const GRAPH_INTERFACE& graph,
    const RowVertexSetImpl<LabelT, typename GRAPH_INTERFACE::vertex_id_t,
                           SET_Ts...>& set,
    const InnerIdProperty<tag_id>& inner_id_prop) {
  return InnerIdGetter<tag_id, typename GRAPH_INTERFACE::vertex_id_t>(
      set.GetVertices());
}

// create inner_id getter for two label vertex set
template <typename GRAPH_INTERFACE, typename LabelT, typename... SET_Ts,
          int tag_id>
static auto create_prop_getter_from_prop_desc(
    const GRAPH_INTERFACE& graph,
    const TwoLabelVertexSetImpl<typename GRAPH_INTERFACE::vertex_id_t, LabelT,
                                SET_Ts...>& set,
    const InnerIdProperty<tag_id>& inner_id_prop) {
  return InnerIdGetter<tag_id, typename GRAPH_INTERFACE::vertex_id_t>(
      set.GetVertices());
}

// create inner id getter for collection.
template <typename GRAPH_INTERFACE, typename COL_T, int tag_id>
static auto create_prop_getter_from_prop_desc(
    const GRAPH_INTERFACE& graph, const Collection<COL_T>& set,
    const InnerIdProperty<tag_id>& inner_id_prop) {
  return InnerIdGetter<tag_id, COL_T>(set.GetVector());
}

// create innerId getter for flat edge set.
template <typename GRAPH_INTERFACE, typename VID_T, typename LabelT,
          size_t NumSrcLabel, typename... EDATA_T, int tag_id>
static auto create_prop_getter_from_prop_desc(
    const GRAPH_INTERFACE& graph,
    const FlatEdgeSet<VID_T, LabelT, NumSrcLabel, EDATA_T...>& set,
    const InnerIdProperty<tag_id>& inner_id_prop) {
  return EdgeSetInnerIdGetter<tag_id, VID_T, EDATA_T...>();
}

// get prop for inner id for idKey
template <typename GRAPH_INTERFACE, typename CTX_HEAD_T, int cur_alias,
          int base_tag, typename... CTX_PREV, int tag_id>
static auto create_prop_getter_from_prop_desc(
    const GRAPH_INTERFACE& graph,
    Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV...>& ctx,
    const InnerIdProperty<tag_id>& inner_id_prop) {
  auto& set = ctx.template GetNode<tag_id>();
  return create_prop_getter_from_prop_desc(graph, set, inner_id_prop);
}

// get prop for common property.
// return a single prop getter
template <typename GRAPH_INTERFACE, typename CTX_T, typename T, int tag_id>
static auto create_prop_getter_from_prop_desc(
    const GRAPH_INTERFACE& graph, CTX_T& ctx,
    const NamedProperty<T, tag_id>& named_property) {
  auto& set = ctx.template GetNode<tag_id>();
  return create_prop_getter_impl<tag_id, T>(set, graph, named_property.name);
}

template <typename GI, typename CTX_T, typename... PROP_DESC, size_t... Is>
static auto create_prop_getters_from_prop_desc(
    const GI& graph, CTX_T& ctx, const std::tuple<PROP_DESC...>& prop_desc,
    std::index_sequence<Is...>) {
  return std::make_tuple(create_prop_getter_from_prop_desc(
      graph, ctx, std::get<Is>(prop_desc))...);
}

template <typename GI, typename CTX_T, typename... PROP_DESC>
static auto create_prop_getters_from_prop_desc(
    const GI& graph, CTX_T& ctx, const std::tuple<PROP_DESC...>& prop_desc) {
  return create_prop_getters_from_prop_desc(
      graph, ctx, prop_desc, std::make_index_sequence<sizeof...(PROP_DESC)>());
}

template <int col_id>
auto create_prop_desc_from_selector(
    const PropertySelector<grape::EmptyType>& selector) {
  return InnerIdProperty<col_id>();
}

template <int col_id, typename T>
auto create_prop_desc_from_selector(const PropertySelector<T>& selector) {
  return NamedProperty<T, col_id>(selector.prop_name_);
}

template <int... in_col_id, typename... SELECTOR, size_t... Ind>
auto create_prop_descs_from_selectors(std::integer_sequence<int, in_col_id...>,
                                      const std::tuple<SELECTOR...>& selectors,
                                      std::index_sequence<Ind...>) {
  return std::make_tuple(
      create_prop_desc_from_selector<in_col_id>(std::get<Ind>(selectors))...);
}

template <int... in_col_id, typename... SELECTOR>
auto create_prop_descs_from_selectors(
    const std::tuple<SELECTOR...>& selectors) {
  return create_prop_descs_from_selectors(
      std::integer_sequence<int, in_col_id...>(), selectors,
      std::make_index_sequence<sizeof...(SELECTOR)>());
}

}  // namespace gs

#endif