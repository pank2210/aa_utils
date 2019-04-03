
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class IntentAccuracy:
    def __init__(self,filepath,ddir="../data/"):
        self.ddir = ddir
        self.filepath = filepath
        self.out_fl = filepath + '.out'
        self.g_fl = 'intent_accu_'
        self.thresh_prob_intrv = 2.0
        self.col_names = ['Date','Workspace','Utterance','IDs','Expected_Intent','Cura_Intent','confidence']

        print("Reading file[%s]" % (ddir + filepath))

        self.df = pd.read_csv(ddir + filepath,encoding="ISO-8859-1")
        self.df.columns = map(lambda x: str.replace(x,' ','_'),self.df.columns)
        self.df = self.df.loc[:,self.col_names]
        #print(self.df.dtypes)

    def calc_accu(self,pred,prob=0.5):
        calc_df =  self.df[self.df.confidence >= prob]
        n = calc_df[(calc_df.Expected_Intent == pred)].Utterance.count()
        #print("*********",prob,"***",calc_df.Utterance.count(),"***",n)
        tp = calc_df[(calc_df.Expected_Intent == pred) & (calc_df.Cura_Intent == pred)].Utterance.count()
        tn = calc_df[(calc_df.Expected_Intent != pred) & (calc_df.Cura_Intent != pred)].Utterance.count()
        fp = calc_df[(calc_df.Expected_Intent != pred) & (calc_df.Cura_Intent == pred)].Utterance.count()
        fn = calc_df[(calc_df.Expected_Intent == pred) & (calc_df.Cura_Intent != pred)].Utterance.count()
        
        return n, tp, tn, fp, fn

    def print_conf_matrix(self,classes):
        thresh_probs = np.arange(0,.9,.05)
        o_fd = open( self.ddir + self.out_fl,"w")
        o_cols = ['label','thresh_prob','n','acc','f1','recall','precision','tp','tn','fp','fn','fpr','tpr']
        o_fd.write("%s\n" % (",".join(o_cols)))

        for pred in classes:
            #thresh_probs = [0,.5,.75]
            for thresh_prob in thresh_probs:
                n, tp, tn, fp, fn = self.calc_accu(pred,thresh_prob)
                f1 = (2*tp)/((2*tp)+fp+fn)
                acc = (tp+tn)/(tp+tn+fp+fn)
                recall = tp/(tp+fn)
                preci = tp/(tp+fp)
                tpr = tp/(tp+fn)
                fpr = fp/(fp+tn)
                print("%s-%.2f - n[%d] tp[%.3f] tn[%.3f] f1[%.3f] acc[%.3f] preci[%.3f] recall[%.3f]" % (pred,thresh_prob,n,tp,fp,f1,acc,preci,recall))
                o_fd.write("%s,%.2f,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n" % (pred,thresh_prob,n,acc,f1,recall,preci,tp, tn, fp, fn,tpr,fpr))
        print("------------------------------------------------------------------")

        o_fd.close()

    def draw_graph(self):
        g_df = pd.read_csv(self.ddir + self.out_fl)

        y0_labels = ['acc','f1','tpr','fpr']
        y0_axis = {}

        labels = g_df['label'].unique()
        #print(labels)
        for label in labels:
            #print("******",label)
            g_df1 = g_df[g_df['label'] == label]
            y0_axis = {}
            fig = plt.figure()
            for y0 in y0_labels:
                y0_axis[y0] = g_df1[y0].tolist()
                x0 = g_df1.thresh_prob.tolist()
                #print(y0_axis[y0])
                plt.plot(x0,y0_axis[y0])
            #plt.gca().legend((y0_labels[0],y0_labels[1]))
            plt.gca().legend(y0_labels)
            fig.suptitle(' Analysis for label ' + label)
            plt.xlabel('Threshold probability')
            plt.ylabel(label)
            plt.xticks(np.arange(0,1,.1))
            plt.yticks(np.arange(0,1,.1))
            plt.savefig(self.ddir + self.g_fl + label + '.png')
            plt.close()


if __name__ == "__main__":
    print("Intent Accuracy started...")

    filepath = "intent_accu1.csv"
    accu = IntentAccuracy(filepath=filepath)
    accu.print_conf_matrix(classes=['churn','change_phone_number'])
    accu.draw_graph()