#########################################
# よく使う作図関数
#########################################
# %%
import plotly.express as px
from plotly.subplots import make_subplots
from typing import List
import numpy as np


def create_line(data, x_axis, cols, hover_cols):
    return px.line(data, x=x_axis, y=cols, hover_data=hover_cols)


def create_point(data, x_axis, cols, hover_cols):
    fig = px.scatter(data, x=x_axis, y=cols, hover_data=hover_cols,)
    fig.update_traces(marker=dict(size=20, symbol='diamond-tall'))
    return fig


def create_bar(data, x_axis, cols, hover_cols):
    fig = px.bar(data, x=x_axis, y=cols, hover_data=hover_cols)
    fig.update_traces(marker_line_width=0)
    return fig


def create_step_backtest_results(return_dict, bt_stng):
    res = return_dict

    step_output_data = res["step_output_data"]
    buy_order_output_data = res["buy_order_output_data"]

    step_output_data = (
        step_output_data
        .assign(idx=step_output_data.index)
        .assign(buy_judge_price=np.where(step_output_data['buy_judge_flg'] == True, step_output_data[bt_stng.price_col], None).astype('float64'))
        .assign(sell_judge_price=np.where(step_output_data['sell_judge_flg'] == True, step_output_data[bt_stng.price_col], None).astype('float64'))
    )
    # step_output_data.columns

    # X軸
    x_axis = 'idx'

    # 価格推移のグラフに合わせて描画
    price_col = bt_stng.price_col
    yaxis_price_line_cols = [price_col]
    yaxis_price_point_cols = ['buy_judge_price', 'sell_judge_price', 'buy_price', 'sell_price', ]

    # エビデンス（どこに追加するかは選ぶ）
    total_fig_row_num, evidence_setting = bt_stng.get_judge_evidence_plot_stng()

    # 出来高
    yaxis_volume_cols = ['volume', 'buy_volume', 'sell_volume']

    # トータルリターン推移
    yaxis_total_return_cols = ['total_return_amount']

    # 勝率推移
    yaxis_rate_cols = ['win_rate']

    # ラベル
    data_label_cols = ['timestamp', 'date']

    settings = [
        dict(fig_row_num=1, type='line', cols=yaxis_price_line_cols),
        dict(fig_row_num=1, type='point', cols=yaxis_price_point_cols),
        dict(fig_row_num=2, type='bar', cols=yaxis_volume_cols),
        dict(fig_row_num=3, type='line', cols=yaxis_total_return_cols),
        dict(fig_row_num=4, type='line', cols=yaxis_rate_cols),
    ] + evidence_setting

    func_dict = dict(
        line=create_line,
        point=create_point,
        bar=create_bar
    )

    figs = [
        dict(
            **stg,
            fig=func_dict[stg['type']](
                data=step_output_data,
                x_axis=x_axis,
                cols=stg["cols"],
                hover_cols=data_label_cols
            )
        )
        for stg in settings
    ]

    fig = make_subplots(rows=total_fig_row_num, cols=1, shared_xaxes=True, row_heights=[0.5, 0.1, 0.2, 0.2])
    for fig_part in figs:
        for d in fig_part["fig"].data:
            fig.add_trace(d, row=fig_part["fig_row_num"], col=1)

    fig.update_layout(
        height=980,
        width=1820,
        title='title',  # グラフタイトル
        font_size=20,  # グラフ全体のフォントサイズ
        hoverlabel_font_size=20,  # ホバーのフォントサイズ
    )

    return fig
