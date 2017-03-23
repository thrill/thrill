#!/usr/bin/env python

#*******************************************************************************
#* examples/stochastic_gradient_descent/gen_data.py
#*
#* Part of Project Thrill - http://project-thrill.org
#*
#* Copyright (C) 2017 Alina Saalfeld <alina.saalfeld@ymail.com>
#*
#* All rights reserved. Published under the BSD-2 license in the LICENSE file.
#******************************************************************************/

import random

D = 1
N = 1000000
SEED = 666


minx = -100
maxx = 100

param_mu = 1
param_sig = 0.2

noise_mu = 1
noise_sig = 0.1

noise_add_sig = 10


random.seed(SEED)
params = [random.gauss(param_mu, param_sig) for x in range(0, D)]


comment = "# Params: "
comment += " ".join(map(str,params))
print comment

print "# D = " + str(D) + ", N = " + str(N) + ", SEED = " + str(SEED)


for i in range(0,N):
	x = [random.uniform(minx, maxx) for x in range(0,D)]
	string = " ".join(map(str,x)) + " "
	value = 0
	for d in range(0,D):
		value += x[d] * params[d]
	value *= random.gauss(noise_mu, noise_sig)
	value += random.gauss(0, noise_add_sig)
	print string + str(value)

