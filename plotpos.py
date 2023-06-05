import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.axes_grid1 import make_axes_locatable

import matplotlib as mpl
import utm

import numpy as np
from matplotlib import cm, colors
# df = pd.read_csv('data.csv')

# t = df.t
# X = df.X
# Y = df.Y
# Z = df.Z

# #plt.scatter(X,Y)
# plt.scatter(t,Z)
# plt.show()
search = '20230424103830'
search = '20230424104410'
# search = '20230424104814'
search = 'van2_greenloop_07feb2023.record'
search = '../CyberData/20230427134945'
search = '../CyberData/20230427135442'
search = '../CyberData/20230427141817'
search = '../CyberData/20230427142512'
search = '../CyberData/20230427145247'
df  = pd.read_csv(search+"gps.csv")
df2 = pd.read_csv(search+"data.csv")

t=df.measurement_time
sol_type = df.sol_type
latitude = df.latitude
longitude = df.longitude
altitude = df.height_msl
num_sats = df.num_sats_in_solution

ins_x_utm = df2.X
ins_y_utm = df2.Y
ins_z_utm = df2.Z
ins_lat = []
ins_lon = []
for p in range(len(ins_x_utm)):
    ins_latlon = utm.to_latlon(ins_x_utm[p], ins_y_utm[p], 17, 'S')
    ins_lat.append(ins_latlon[0])
    ins_lon.append(ins_latlon[1])

# fig, ax1 = plt.subplots()
# ax2 = ax1.twinx()
# ax1.scatter(t,sol_type,color='g')
# ax2.scatter(t,altitude)

fig, ax1  = plt.subplots(2,1)
fig2, ax2 = plt.subplots(1,1)

ax1[0].scatter(longitude,latitude,c=sol_type, marker='o', alpha=0.3)
ax1[1].scatter(longitude,latitude,c=num_sats, marker='^', alpha=0.3)

ax1[0].grid(True)
ax1[1].grid(True)

sc1 = ax1[0].set_aspect('equal', 'box')
sc2 = ax1[1].set_aspect('equal', 'box')

#cax = plt.axes([0.85, 0.1, 0.075, 0.8])
#plt.colorbar(cax=cax)
divider = make_axes_locatable(ax1[0])
cax = divider.append_axes("right", size="5%", pad=0.05)

norm = colors.Normalize(np.min(sol_type),np.max(sol_type))
cbar1 = fig.colorbar(sc1, ax=ax1[1], label='Solution Type', norm=norm, cax=cax)
# cbar1.set_ticklabels(['Narrow INT', 'PSRDIFF', 'Narrow FLOAT'])
# 
divider = make_axes_locatable(ax1[1])
cax = divider.append_axes("right", size="5%", pad=0.05)


norm2 = colors.Normalize(np.min(num_sats),np.max(num_sats))
cbar2=fig.colorbar(sc2, ax=ax1[1], label='SV Count', norm=norm2, cax=cax)

ax2.plot(ins_lat,  ins_lon,c='black', label='100 Hz INS?', alpha=0.5)
ax2.scatter(latitude, longitude, marker='^',  label='20 Hz GNSS', alpha=0.5)
ax2.set_xlabel('Latitude')
ax2.set_ylabel('Longitude')
ax2.axis('equal')
ax2.grid(True)
ax2.legend()

plt.show()
#sol_type
#50 NARROW_INT
#17 PSRDIFF
#34 NARROW_FLOAT