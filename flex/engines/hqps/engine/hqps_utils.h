#ifndef GRAPHSCOPE_ENGINE_QPS_UTILS_H_
#define GRAPHSCOPE_ENGINE_QPS_UTILS_H_

#include <array>
#include <optional>
#include <queue>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flex/engines/hqps/engine/params.h"
#include "flex/storages/mutable_csr/fragment/ts_property_fragment.h"
#include "flex/storages/mutable_csr/property/column.h"
#include "flex/storages/mutable_csr/types.h"

namespace gs {

template <typename T>
struct is_tuple : std::false_type {};

template <typename... T>
struct is_tuple<std::tuple<T...>> : std::true_type {};

template <typename T, typename Enable = void>
struct tuple_size {};

template <typename T>
struct tuple_size<T, typename std::enable_if<is_tuple<T>::value>::type> {
  static constexpr size_t value = std::tuple_size<T>::value;
};

template <typename T>
struct tuple_size<T, typename std::enable_if<std::is_pod<T>::value>::type> {
  static constexpr size_t value = 1;
};

// check whether the group key uses property
template <typename KEY_ALIAS_T>
struct group_key_on_property : public std::true_type {};

template <int in_tag_id, int res_alias_id>
struct group_key_on_property<
    AliasTagProp<in_tag_id, res_alias_id, grape::EmptyType>>
    : public std::false_type {};

// check edge_dir and vopt consistency
inline bool check_edge_dir_consist_vopt(const Direction& dir, VOpt vopt) {
  if (dir == Direction::Out) {
    return vopt == VOpt::End || vopt == VOpt::Other;
  } else if (dir == Direction::In) {
    return vopt == VOpt::Start || vopt == VOpt::Other;
  } else if (dir == Direction::Both) {
    return vopt == VOpt::Other;
  }
  LOG(FATAL) << "Invalid direction: " << dir;
  return false;
}

// customized operator
// 0. WithIn
const struct WithIn_ {
} WithIn;

template <typename T>
struct WithProxy {
  WithProxy(const T& t) : t_(t) {}
  const T& t_;
};

template <typename T>
WithProxy<T> operator<(const T& lhs, const WithIn_& rhs) {
  return WithProxy<T>(lhs);
}

template <
    typename T, size_t N,
    typename std::enable_if<std::is_pod_v<T> && (N == 1)>::type* = nullptr>
bool operator>(const WithProxy<T>& lhs, const std::array<T, N>& rhs) {
  return lhs.t_ == rhs[0];
}

template <typename T, size_t N,
          typename std::enable_if<std::is_pod_v<T> && (N > 1)>::type* = nullptr>
bool operator>(const WithProxy<T>& lhs, const std::array<T, N>& rhs) {
  return rhs.end() != std::find(rhs.begin(), rhs.end(), lhs.t_);
}

template <
    typename T, size_t N,
    typename std::enable_if<std::is_pod_v<T> && (N == 0)>::type* = nullptr>
bool operator>(const WithProxy<T>& lhs, const std::array<T, N>& rhs) {
  return false;
}

template <
    std::size_t nth, std::size_t... Head, std::size_t... Tail,
    typename... Types,
    typename std::enable_if<(nth + 1 != sizeof...(Types))>::type* = nullptr>
constexpr auto remove_nth_element_impl(std::index_sequence<Head...>,
                                       std::index_sequence<Tail...>,
                                       const std::tuple<Types...>& tup) {
  return std::tuple{std::get<Head>(tup)...,
                    // We +1 to refer one element after the one removed
                    std::get<Tail + nth + 1>(tup)...};
}

template <
    std::size_t nth, std::size_t... Head, std::size_t... Tail,
    typename... Types,
    typename std::enable_if<(nth + 1 == sizeof...(Types))>::type* = nullptr>
constexpr auto remove_nth_element_impl(std::index_sequence<Head...>,
                                       std::index_sequence<Tail...>,
                                       const std::tuple<Types...>& tup) {
  return std::tuple{std::get<Head>(tup)...};
}

template <std::size_t nth, typename... Types>
constexpr auto remove_nth_element(const std::tuple<Types...>& tup) {
  static_assert(nth < sizeof...(Types));
  return remove_nth_element_impl<nth>(
      std::make_index_sequence<nth>(),
      std::make_index_sequence<sizeof...(Types) - nth - 1>(), tup);
}

template <size_t ith, size_t jth, typename... Types>
constexpr auto remove_ith_jth_element(const std::tuple<Types...>& tup) {
  static_assert(ith < sizeof...(Types));
  static_assert(jth < sizeof...(Types));
  static_assert(ith != jth);
  if constexpr (ith < jth) {
    return remove_nth_element<ith>(remove_nth_element<jth>(tup));
  } else {
    return remove_nth_element<jth>(remove_nth_element<ith>(tup));
  }
}

template <size_t I, typename T>
struct remove_ith_type {};

template <typename T, typename... Ts>
struct remove_ith_type<0, std::tuple<T, Ts...>> {
  typedef std::tuple<Ts...> type;
};

template <size_t I, typename T, typename... Ts>
struct remove_ith_type<I, std::tuple<T, Ts...>> {
  typedef decltype(std::tuple_cat(
      std::declval<std::tuple<T>>(),
      std::declval<typename remove_ith_type<I - 1, std::tuple<Ts...>>::type>()))
      type;
};

// I != J
template <size_t I, size_t J, typename T, typename Void = void>
struct remove_ith_jth_type {};

template <size_t I, size_t J, typename T, typename... Ts>
struct remove_ith_jth_type<I, J, std::tuple<T, Ts...>,
                           typename std::enable_if<(I < J)>::type> {
  using first_type = typename remove_ith_type<I, std::tuple<T, Ts...>>::type;
  using type = typename remove_ith_type<J - 1, first_type>::type;
};

template <size_t I, size_t J, typename T, typename... Ts>
struct remove_ith_jth_type<I, J, std::tuple<T, Ts...>,
                           typename std::enable_if<(I > J)>::type> {
  using type = typename remove_ith_jth_type<J, I, std::tuple<T, Ts...>>::type;
};

template <typename VID_T, typename... T>
struct Edge;

template <
    size_t Is, typename... PROP_T,
    typename std::enable_if<Is<sizeof...(PROP_T) - 1>::type* = nullptr> void
        props_to_string_array(
            std::tuple<PROP_T...>& props,
            std::array<std::string, std::tuple_size_v<std::tuple<PROP_T...>>>&
                res) {
  res[Is] = std::get<Is>(props).property_name;
  props_to_string_array<Is + 1>(props, res);
}

template <size_t Is, typename... PROP_T,
          typename std::enable_if<Is == sizeof...(PROP_T) - 1>::type* = nullptr>
void props_to_string_array(
    std::tuple<PROP_T...>& props,
    std::array<std::string, std::tuple_size_v<std::tuple<PROP_T...>>>& res) {
  res[Is] = std::get<Is>(props).property_name;
}
template <typename... PROP_T>
auto propsToStringArray(std::tuple<PROP_T...>& props) {
  std::array<std::string, sizeof...(PROP_T)> res;
  props_to_string_array<0>(props, res);
  return res;
}

template <int I, class T>
struct tuple_element;

// recursive case
template <int I, class Head, class... Tail>
struct tuple_element<I, std::tuple<Head, Tail...>>
    : gs::tuple_element<I - 1, std::tuple<Tail...>> {};

// base case
template <class Head, class... Tail>
struct tuple_element<0, std::tuple<Head, Tail...>> {
  using type = Head;
};

template <class Head, class... Tail>
struct tuple_element<-1, std::tuple<Head, Tail...>>
    : gs::tuple_element<sizeof...(Tail) - 1, std::tuple<Tail...>> {};
template <class Head>
struct tuple_element<-1, std::tuple<Head>> {
  using type = Head;
};

template <typename... T>
auto unwrap_future_tuple(std::tuple<T...>&& tuple) {
  return unwrap_future_tuple(std::move(tuple),
                             std::make_index_sequence<sizeof...(T)>());
}
template <typename... T, size_t... Is>
auto unwrap_future_tuple(std::tuple<T...>&& tuple, std::index_sequence<Is...>) {
  return std::make_tuple(std::move(std::get<Is>(tuple).get0())...);
}

inline std::vector<offset_t> merge_union_offset(std::vector<offset_t>& a,
                                                std::vector<offset_t>& b) {
  CHECK(a.size() == b.size() && a.size() > 0);
  std::vector<offset_t> res;
  res.reserve(a.size());
  res[0] = a[0] + b[0];
  for (auto i = 1; i < a.size(); ++i) {
    res[i] = res[i - 1] + a[i] - a[i - 1] + b[i] - b[i - 1];
  }
  return res;
}

inline auto make_offset_vector(size_t m, size_t n) {
  // VLOG(10) << "m: " << m << ",n:" << n;
  std::vector<std::vector<size_t>> offsets;
  //[0,m)
  for (auto i = 0; i < m; ++i) {
    // [0, n]
    std::vector<offset_t> cur(n + 1, 0);
    for (auto j = 0; j <= n; ++j) {
      cur[j] = j;
    }
    offsets.emplace_back(std::move(cur));
  }
  // VLOG(10) << "offsets.size() " << offsets.size();
  return offsets;
}

// first n ele in tuple type

template <int n, typename In, typename... Out>
struct first_n_impl;

template <int n, typename First, typename... Other, typename... Out>
struct first_n_impl<n, std::tuple<First, Other...>, Out...> {
  typedef
      typename first_n_impl<n - 1, std::tuple<Other...>, Out..., First>::type
          type;  // move first input to output.
};

// need First, Other... here to resolve ambiguity on n = 0
template <typename First, typename... Other, typename... Out>
struct first_n_impl<0, std::tuple<First, Other...>, Out...> {
  typedef typename std::tuple<Out...> type;  // stop if no more elements needed
};

// explicit rule for empty tuple because of First, Other... in the previous
// rule.
// actually it's for n = size of tuple
template <typename... Out>
struct first_n_impl<0, std::tuple<>, Out...> {
  typedef typename std::tuple<Out...> type;
};

// template <int n, typename... Others>
// using first_n = first_n_impl<n, std::tuple<Others...>>;

template <int n, typename T>
struct first_n;

template <int n, typename... T>
struct first_n<n, std::tuple<T...>> {
  using type = typename first_n_impl<n, std::tuple<T...>>::type;
};

template <size_t l, typename T, size_t... Is>
constexpr auto tuple_slice_impl(T&& t, std::index_sequence<Is...>) {
  return std::forward_as_tuple(std::get<l + Is>(std::forward<T>(t))...);
}

template <size_t l, size_t r, typename T>
constexpr auto tuple_slice(T&& t) {
  static_assert(r >= l, "invalid slice");
  static_assert(std::tuple_size<std::decay_t<T>>::value >= r,
                "slice index out of bounds");
  return tuple_slice_impl<l>(std::forward<T>(t),
                             std::make_index_sequence<r - l>{});
}

template <int Is, typename... T,
          typename std::enable_if<(Is >= 0)>::type* = nullptr>
inline auto get_from_tuple(std::tuple<T...>& tuple) {
  return std::get<Is>(tuple);
}

template <int Is, typename... T,
          typename std::enable_if<(Is == -1)>::type* = nullptr>
inline auto get_from_tuple(std::tuple<T...>& tuple) {
  static constexpr size_t num = sizeof...(T);
  return std::get<num - 1>(tuple);
}
template <int Is, typename... T,
          typename std::enable_if<(Is >= 0)>::type* = nullptr>
inline const auto& get_from_tuple(const std::tuple<T...>& tuple) {
  return std::get<Is>(tuple);
}

template <int Is, typename... T,
          typename std::enable_if<(Is == -1)>::type* = nullptr>
inline const auto& get_from_tuple(const std::tuple<T...>& tuple) {
  static constexpr size_t num = sizeof...(T);
  return std::get<num - 1>(tuple);
}

// vertex/edge property associate with type
template <typename T, size_t N, typename FUNC_T, size_t... Is,
          typename RET_T = typename std::result_of<FUNC_T(T)>::type>
auto transform_array_impl(std::array<T, N>&& array, FUNC_T&& func,
                          std::index_sequence<Is...>) {
  return std::array<RET_T, N>{std::move(func(std::move(array[Is])))...};
}

template <typename T, size_t N, typename FUNC_T>
auto transform_array(std::array<T, N>&& array, FUNC_T&& func) {
  return transform_array_impl(std::move(array), std::move(func),
                              std::make_index_sequence<N>());
}

template <typename... T, typename FUNC_T, size_t... Is,
          typename RET_T = typename std::result_of<FUNC_T(T&...)>::type>
auto transform_tuple_impl(const std::tuple<T...>&& tuple, FUNC_T&& func,
                          std::index_sequence<Is...>) {
  return std::make_tuple(
      std::move(func(Is, std::move(std::get<Is>(tuple))))...);
}

template <typename... T, typename FUNC_T>
auto transform_tuple(const std::tuple<T...>&& tuple, FUNC_T&& func) {
  static constexpr size_t N = sizeof...(T);
  return transform_tuple_impl(std::move(tuple), std::move(func),
                              std::make_index_sequence<N>());
}

template <typename FUNC, typename... T>
bool apply_on_tuple(const FUNC& func, const std::tuple<T...>& tuple) {
  return apply_on_tuple_impl(func, tuple,
                             std::make_index_sequence<sizeof...(T)>());
}

template <typename FUNC, typename... T, size_t... Is>
bool apply_on_tuple_impl(const FUNC& func, const std::tuple<T...>& tuple,
                         std::index_sequence<Is...>) {
  return func(std::get<Is>(tuple)...);
}

template <typename T, size_t N, typename FUNC_T, size_t... Is,
          typename RET_T = typename std::result_of<FUNC_T(T&)>::type>
auto apply_array_impl(const std::array<T, N>& array, FUNC_T&& func,
                      std::index_sequence<Is...>) {
  return std::array<RET_T, N>{std::move(func(array[Is]))...};
}

template <typename T, size_t N, typename FUNC_T>
auto apply_array(const std::array<T, N>& array, FUNC_T&& func) {
  return apply_array_impl(array, std::move(func),
                          std::make_index_sequence<N>());
}

template <typename... T, typename FUNC_T, typename... OTHER_ARGS, size_t... Is>
void apply_tuple_impl(const std::tuple<T...>& tuple, const FUNC_T& func,
                      std::index_sequence<Is...>, OTHER_ARGS&... other_args) {
  ((func(std::get<Is>(tuple), std::forward<OTHER_ARGS>(other_args)...)), ...);
}

template <typename... T, typename FUNC_T, typename... OTHER_ARGS>
auto apply_tuple(const std::tuple<T...>& tuple, const FUNC_T& func,
                 OTHER_ARGS&... other_args) {
  static constexpr size_t N = sizeof...(T);
  return apply_tuple_impl(tuple, func, std::make_index_sequence<N>(),
                          std::forward<OTHER_ARGS>(other_args)...);
}

template <typename Dest = void, typename... Args>
constexpr auto make_array(Args&&... args) {
  if constexpr (std::is_same<void, Dest>::value) {
    return std::array<std::common_type_t<std::decay_t<Args>...>,
                      sizeof...(Args)>{{std::forward<Args>(args)...}};
  } else {
    return std::array<Dest, sizeof...(Args)>{{std::forward<Args>(args)...}};
  }
}

template <typename T>
using DataTupleT = typename T::data_tuple_t;

// T must be tuple
template <typename... T>
using tuple_cat_t = decltype(std::tuple_cat(std::declval<T>()...));

template <class T>
struct is_shared_ptr : std::false_type {};

template <class T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

template <class T>
struct is_vector : std::false_type {};

template <class T>
struct is_vector<std::vector<T>> : std::true_type {};

template <typename>
struct is_pair : std::false_type {};

template <typename T, typename U>
struct is_pair<std::pair<T, U>> : std::true_type {};

// template <typename>
// struct CanToString : std::false_type {};

// template <typename VID_T, typename... Ts>
// struct CanToString<Edge<VID_T, Ts...>> : std::true_type {};

template <std::size_t N, std::size_t... Seq>
constexpr std::index_sequence<N + Seq...> add(std::index_sequence<Seq...>) {
  return {};
}

template <size_t M, size_t N>
struct NumberLarger {
  static const bool value = (M >= N);
};

template <std::size_t Min, std::size_t Max>
using make_index_range =
    decltype(add<Min>(std::make_index_sequence<Max - Min>()));

template <typename T1, typename T2>
struct TupleCatT {
  using tuple_cat_t =
      decltype(std::tuple_cat(std::declval<T1>(), std::declval<T2>()));
};

template <typename T2>
struct TupleCatT<std::tuple<grape::EmptyType>, T2> {
  using tuple_cat_t = decltype(std::tuple_cat(std::declval<T2>()));
};

template <typename T1>
struct TupleCatT<T1, std::tuple<grape::EmptyType>> {
  using tuple_cat_t = decltype(std::tuple_cat(std::declval<T1>()));
};

// We use DataGetter to wrapper getting element for a column/vector
template <typename T>
struct ColDataGetter {
  using value_type = T;
  using element_type = T;
  std::shared_ptr<gs::TypedColumn<T>> col_;
  std::string col_name_;
  ColDataGetter(std::shared_ptr<gs::TypedColumn<T>> ptr, std::string col_name)
      : col_(ptr), col_name_(col_name) {}

  inline element_type Get(size_t index) const { return col_->get_view(index); }
  inline element_type Get(size_t index, vid_t vid) const {
    return col_->get_view(vid);
  }
};

struct OidGetter {
  using value_type = oid_t;
  using element_type = gs::oid_t;
  ConstVertexStore vertex_store_;
  OidGetter(ConstVertexStore&& vertex_store)
      : vertex_store_(std::move(vertex_store)) {}

  inline element_type Get(size_t index) const {
    return vertex_store_.get_id(index);
  }
  inline element_type Get(size_t index, vid_t vid) const {
    return vertex_store_.get_id(vid);
  }
};

// template <typename T, int N>
// struct ColMeta {};

// template <typename T>
// struct ColMeta<T, 0> {
//   using value_type = T;
//   using GetterType = ColDataGetter<T>;
//   std::string col_name_;
//   const TSPropertyFragment& frag_;
//   ColMeta(const TSPropertyFragment& frag, std::string col_name)
//       : frag_(frag), col_name_(col_name) {}

//   ColDataGetter<T> CreateGetter(label_t label) {
//     auto vertex_store = frag_.GetVertexStore(label);
//     auto base = vertex_store.get_property_column(col_name_);
//     auto casted = std::dynamic_pointer_cast<gs::TypedColumn<T>>(base);
//     CHECK(casted) << "Fail to obtain col getter for " << col_name_;
//     return ColDataGetter<T>(casted, col_name_);
//   }
// };

// oid meta
// template <typename T>
// struct ColMeta<T, 1> {
//   using value_type = T;
//   using GetterType = OidGetter;
//   const TSPropertyFragment& frag_;

//   ColMeta(const TSPropertyFragment& frag) : frag_(frag) {}
//   OidGetter CreateGetter(label_t label) {
//     return OidGetter(std::move(frag_.GetVertexStore(label)));
//   }
// };

// using OidColMeta = ColMeta<oid_t, 1>;

// template <typename T>
// using PropertyColMeta = ColMeta<T, 0>;

template <typename... ColMetas, size_t... Is>
auto make_getter_tuple(label_t label, std::tuple<ColMetas...>&& tuple,
                       std::index_sequence<Is...>) {
  return std::make_tuple(std::get<Is>(tuple).CreateGetter(label)...);
}

template <typename T>
using ValueTypeOf = typename T::value_type;

template <typename T>
using SharedPtrTypeOf = std::shared_ptr<gs::TypedColumn<T>>;

template <typename T>
using GetterTypeOf = typename T::GetterType;

template <typename GETTER_T>
using ElementTypeOf = typename GETTER_T::element_type;

template <typename T>
using DataOfColumnPtr = typename T::element_type::value_type;

template <typename T>
using IterOf = typename T::iterator;

template <typename... COL_META>
auto get_column_tuples(ConstVertexStore& vertex_store, COL_META... col_metas) {
  auto func = [&](auto& col_meta) {
    return col_meta.CreateGetter(vertex_store);
  };
  return std::make_tuple(func(col_metas)...);
}

template <typename TUPLE_T, typename CMP>
using PQ_T = std::priority_queue<TUPLE_T, std::vector<TUPLE_T>, CMP>;

template <std::size_t i, typename COL>
struct SingleColumn {
  COL col_;
};

// Definition
template <std::size_t i, typename... COLS>
struct ColumnAccessorImpl;

// Empty Accessor.
template <std::size_t i>
struct ColumnAccessorImpl<i> {};

// Recurvise
template <std::size_t i, typename FIRST, typename... OTHER>
struct ColumnAccessorImpl<i, FIRST, OTHER...>
    : public SingleColumn<i, FIRST>,
      public ColumnAccessorImpl<i + 1, OTHER...> {};

// multiple single columns.

// Obtain a reference
template <std::size_t i, typename FIRST, typename... OTHER>
FIRST& Get(ColumnAccessorImpl<i, FIRST, OTHER...>& tuple) {
  // Fully qualified name for the member, to find the right one
  // (they are all called `value`).
  return tuple.SingleColumn<i, FIRST>::col_;
}

template <typename... COLS>
using ColumnAccessor = ColumnAccessorImpl<0, COLS...>;

// Make COlumnAccessor like make tuple

enum class OperatorType {
  kAuxilia = 0,
  kEdgeExpand = 1,
  kGetV = 2,
  kProject = 3,
  kSink = 4,
};

enum class Cmp {
  kEQ = 0,
  kLT = 1,
  kGT = 2,
  kLE = 3,
  kGE = 4,
  kINSIDE = 5,
  kOUTSIDE = 6,
  kWITHIN = 7,
  kWITHOUT = 8,
};

enum class SourceType { kVertex = 0, kEdge = 1 };
enum class EntryType {
  kVertexEntry = 0,
  kEdgeEntry = 1,
  kObjectEntry = 2,
  kPathEntry = 3,
  kProjectedVertexEntry = 4,
  kProjectedEdgeEntry = 5,
};

template <typename PRIORITY_QUEUE_T>
static typename PRIORITY_QUEUE_T::container_type priority_queue_to_vec(
    PRIORITY_QUEUE_T& pq, bool reversed = false) {
  auto pq_size = pq.size();
  typename PRIORITY_QUEUE_T::container_type res;
  res.reserve(pq_size);
  for (int i = 0; i < pq_size; ++i) {
    res.emplace_back(pq.top());
    pq.pop();
  }
  return res;
}

/*
template <typename... T>
std::string to_string(const T&... ts) {
  return std::apply([](const auto&... t) { return to_string(t...); }, ts...);
}
*/
template <typename T>
struct to_string_impl {
  static std::string to_string(const T& t) { return t.to_string(); }
};

template <typename T>
struct to_string_impl<std::vector<T>> {
  static inline std::string to_string(const std::vector<T>& vec) {
    std::ostringstream ss;
    //    ss << "Vec[";
    if (vec.size() > 0) {
      for (int i = 0; i < vec.size() - 1; ++i) {
        ss << to_string_impl<T>::to_string(vec[i]) << ",";
      }
      ss << to_string_impl<T>::to_string(vec[vec.size() - 1]);
    }
    //    ss << "]";
    return ss.str();
  }
};

template <typename T, size_t N>
struct to_string_impl<std::array<T, N>> {
  static inline std::string to_string(const std::array<T, N>& empty) {
    std::stringstream ss;
    for (auto i : empty) {
      ss << i << ",";
    }
    return ss.str();
  }
};

template <>
struct to_string_impl<Dist> {
  static inline std::string to_string(const Dist& empty) {
    return std::to_string(empty.dist);
  }
};

template <>
struct to_string_impl<Date> {
  static inline std::string to_string(const Date& empty) {
    return std::to_string(empty.milli_second);
  }
};

template <>
struct to_string_impl<std::string_view> {
  static inline std::string to_string(const std::string_view& empty) {
    return std::string(empty);
  }
};

template <>
struct to_string_impl<grape::EmptyType> {
  static inline std::string to_string(const grape::EmptyType& empty) {
    return "";
  }
};

template <>
struct to_string_impl<uint8_t> {
  static inline std::string to_string(const uint8_t& empty) {
    return std::to_string((int32_t) empty);
  }
};

template <>
struct to_string_impl<int64_t> {
  static inline std::string to_string(const int64_t& empty) {
    return std::to_string(empty);
  }
};

template <>
struct to_string_impl<bool> {
  static inline std::string to_string(const bool& empty) {
    return std::to_string(empty);
  }
};

template <>
struct to_string_impl<unsigned long> {
  static inline std::string to_string(const unsigned long& empty) {
    return std::to_string(empty);
  }
};

template <>
struct to_string_impl<int32_t> {
  static inline std::string to_string(const int32_t& empty) {
    return std::to_string(empty);
  }
};

template <>
struct to_string_impl<uint32_t> {
  static inline std::string to_string(const uint32_t& empty) {
    return std::to_string(empty);
  }
};

template <>
struct to_string_impl<double> {
  static inline std::string to_string(const double& empty) {
    return std::to_string(empty);
  }
};

template <>
struct to_string_impl<std::string> {
  static inline std::string to_string(const std::string& empty) {
    return empty;
  }
};

template <>
struct to_string_impl<Direction> {
  static inline std::string to_string(const Direction& opt) {
    if (opt == Direction::In) {
      return "In";
    } else if (opt == Direction::Out) {
      return "Out";
    } else {
      return "Both";
    }
  }
};

template <>
struct to_string_impl<ResultOpt> {
  static inline std::string to_string(const ResultOpt& result_opt) {
    if (result_opt == ResultOpt::AllV) {
      return "AllV";
    } else {
      return "EndV";
    }
  }
};

template <>
struct to_string_impl<PathOpt> {
  static inline std::string to_string(const PathOpt& result_opt) {
    if (result_opt == PathOpt::Arbitrary) {
      return "Arbitrary";
    } else {
      return "Simple";
    }
  }
};

template <>
struct to_string_impl<JoinKind> {
  static inline std::string to_string(const JoinKind& result_opt) {
    if (result_opt == JoinKind::AntiJoin) {
      return "AntiJoin";
    } else if (result_opt == JoinKind::Semi) {
      return "Semi";
    } else {
      return "InnerJoin";
    }
  }
};

template <>
struct to_string_impl<gs::VOpt> {
  static inline std::string to_string(const gs::VOpt& opt) {
    switch (opt) {
    case gs::VOpt::Start:
      return "Start";
    case gs::VOpt::End:
      return "End";
    case gs::VOpt::Other:
      return "Other";
    case gs::VOpt::Both_V:
      return "Both";
    case gs::VOpt::Itself:
      return "Itself";
    }
    LOG(ERROR) << "Should not reach here";
    return "";
  }
};

template <typename... Args>
struct to_string_impl<std::tuple<Args...>> {
  static inline std::string to_string(const std::tuple<Args...>& t) {
    std::string result;
    result += "tuple<";
    std::apply(
        [&result](const auto&... v) {
          ((result +=
            (to_string_impl<std::remove_const_t<
                 std::remove_reference_t<decltype(v)>>>::to_string(v)) +
            ","),
           ...);
        },
        t);
    result += ">";
    return result;
  }
};

template <typename A, typename B>
struct to_string_impl<std::pair<A, B>> {
  static inline std::string to_string(const std::pair<A, B>& t) {
    std::stringstream ss;
    ss << "pair<" << to_string_impl<A>::to_string(t.first) << ","
       << to_string_impl<B>::to_string(t.second) << ">";
    return ss.str();
  }
};

template <typename T>
std::string to_string(const T& t) {
  return to_string_impl<T>::to_string(t);
}

/*
template <typename EDATA_T>
std::string to_string<gs::Edge<EDATA_T>>(const gs::Edge<EDATA_T>& edge) {
  return gs::to_string(edge.src) + "->" + gs::to_string(edge.dst) + "(" +
gs::to_string(edge.edata) +")";
}
*/

// template <>
// inline std::string to_string<gs::EdgeStrategy>(const gs::EdgeStrategy& t) {
//   if (t == gs::EdgeStrategy::kNone) {
//     return "kNone";
//   } else if (t == gs::EdgeStrategy::kSingle) {
//     return "kSingle";
//   } else
//     return "kMultiple";
// }

// template <>
// inline std::string to_string<std::vector<std::string_view>>(
//     const std::vector<std::string_view>& t) {
//   std::string result;
//   result += "vec{";
//   for (auto v : t) {
//     result += v;
//     result += ",";
//   }
//   result += "}";
//   return result;
// }

// template <>
// inline std::string to_string<std::string_view>(const std::string_view& t) {
//   return std::string(t);
// }  // namespace gs

/*
template <typename T1, typename... T>
std::string to_string(const T1& t1, const T&... ts) {
  return gs::to_string(t1) + "," + gs::to_string(ts...);
}
*/

// template <typename... Args>
// inline std::string to_string(const std::vector<std::tuple<Args...>>& t) {
//   std::string result;
//   result += "vector<";
//   for (auto tuple : t) {
//     result += gs::to_string(tuple) + ",";
//   }
//   result += ">";
//   return result;
// }

template <typename VID_T, typename... EDATA_T>
struct Edge {
  VID_T src, dst;
  const std::tuple<EDATA_T...>& edata;
  Edge(VID_T s, VID_T d, const std::tuple<EDATA_T...>& data)
      : src(s), dst(d), edata(data) {}
  std::string to_string() const {
    return std::to_string(src) + "->" + std::to_string(dst) + "(" +
           gs::to_string(edata) + ")";
  }
};

template <typename VID_T>
struct Edge<VID_T, grape::EmptyType> {
  VID_T src, dst;
  grape::EmptyType edata;
  Edge(vid_t s, vid_t d) : src(s), dst(d) {}
  std::string to_string() const {
    return std::to_string(src) + "->" + std::to_string(dst) + "(" + ")";
  }
};

/*
static std::string to_string(const algebra::GetV::VOpt& opt) {
  switch (opt) {
  case algebra::GetV::Start:
    return "Start";
  case algebra::GetV::End:
    return "End";
  case algebra::GetV::Other:
    return "Other";
  case algebra::GetV::Both:
    return "Both";
  }
  LOG(ERROR) << "Should not reach here";
  return "";
}
static Direction parseDirectionFromPb(
    const algebra::EdgeExpand::Direction& dir_pb) {
  Direction direction;
  switch (dir_pb) {
  case algebra::EdgeExpand::OUT:
    direction = Direction::Out;
    break;
  case algebra::EdgeExpand::IN:
    direction = Direction::In;
    break;
  case algebra::EdgeExpand::BOTH:
    direction = Direction::Both;
    break;
  }
  return direction;
}


*/
struct QPSError {
  std::string message;
  explicit QPSError(std::string msg) : message(std::move(msg)) {}

  std::string GetMessage() { return message; }
};

class QPSException : public std::exception {
 public:
  explicit QPSException(std::string&& error_msg)
      : std::exception(), _err_msg(error_msg) {}
  ~QPSException() override = default;

  const char* what() const noexcept override { return _err_msg.c_str(); }

 private:
  std::string _err_msg;
};

inline void printSchema(const gs::TSPropertyFragment& frag) {
  for (auto i = 0; i < frag.schema().vertex_label_num(); ++i) {
    VLOG(10) << "ind" << i << ":vertex label "
             << frag.schema().get_vertex_label_name(i);
  }
  for (auto i = 0; i < frag.schema().edge_label_num(); ++i) {
    VLOG(10) << "ind" << i << ": edge label "
             << frag.schema().get_edge_label_name(i);
  }
}

}  // namespace gs

#endif  // GRAPHSCOPE_ENGINE_QPS_UTILS_H_